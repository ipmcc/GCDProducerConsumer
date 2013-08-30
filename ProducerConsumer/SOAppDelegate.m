//
//  SOAppDelegate.m
//  ProducerConsumer
//
//  Created by Ian McCullough on 8/20/13.
//  Copyright (c) 2013 Ian McCullough. All rights reserved.
//

#import "SOAppDelegate.h"
#import <libkern/OSAtomic.h>
#import <objc/runtime.h>

static void DieOnError(int error);
static NSString* NSStringFromDispatchData(dispatch_data_t data);
static dispatch_data_t FrameDataFromAccumulator(dispatch_data_t* accumulator);
static CVReturn MyDisplayLinkCallback(CVDisplayLinkRef displayLink, const CVTimeStamp* now, const CVTimeStamp* outputTime, CVOptionFlags flagsIn, CVOptionFlags* flagsOut, void* displayLinkContext);

static const NSUInteger kFramesToOverlap = 15;
static const NSUInteger kMaxFramesInPipelineAtOnce = kFramesToOverlap * 3;

static void EnqueueOrderedParallelizableWork(dispatch_block_t workBlock, dispatch_block_t completionBlock, dispatch_queue_t completionQueue);

@implementation SOAppDelegate
{
    // Display link state
    CVDisplayLinkRef mDisplayLink;
    
    // State for our file reading process -- protected via mFrameReadQueue
    dispatch_queue_t mFrameReadQueue;
    NSUInteger mFileIndex; // keep track of what file we're reading
    dispatch_data_t mFrameReadAccumulator; // keep track of left-over data across read operations

    // State for processing raw frame data delivered by the read process - protected via mFrameDataProcessingQueue
    dispatch_queue_t mFrameDataProcessingQueue;
    NSMutableArray* mFilesForOverlapping;
    NSMutableArray* mFrameArraysForOverlapping;

    // State for blending frames (or passing them through)
    dispatch_queue_t mFrameBlendingQueue;
    
    // Delivery state
    dispatch_queue_t mFrameDeliveryQueue; // Is suspended/resumed to deliver one frame at a time
    dispatch_queue_t mFrameDeliveryStateQueue; // Protects access to the iVars
    dispatch_data_t mDeliveredFrame; // Data of the frame that has been delivered, but not yet picked up by the CVDisplayLink
    NSInteger mLastFrameDelivered; // Counter of frames delivered
    NSInteger mLastFrameDisplayed; // Counter of frames displayed

    volatile int32_t mNumFramesInBuffer;
    dispatch_queue_t mLogQueue;
}

- (void)applicationDidFinishLaunching:(NSNotification *)aNotification
{
    mFileIndex = 1;
    mLastFrameDelivered = -1;
    mLastFrameDisplayed = -1;

    mFrameReadQueue = dispatch_queue_create("mFrameReadQueue", DISPATCH_QUEUE_SERIAL);
    mFrameDataProcessingQueue = dispatch_queue_create("mFrameDataProcessingQueue", DISPATCH_QUEUE_SERIAL);
    mFrameBlendingQueue = dispatch_queue_create("mFrameBlendingQueue", DISPATCH_QUEUE_SERIAL);
    mFrameDeliveryQueue = dispatch_queue_create("mFrameDeliveryQueue", DISPATCH_QUEUE_SERIAL);
    mFrameDeliveryStateQueue = dispatch_queue_create("mFrameDeliveryStateQueue", DISPATCH_QUEUE_SERIAL);
    mLogQueue = dispatch_queue_create("mLogQueue", DISPATCH_QUEUE_SERIAL);
    
    CVDisplayLinkCreateWithActiveCGDisplays(&mDisplayLink);
    CVDisplayLinkSetOutputCallback(mDisplayLink, &MyDisplayLinkCallback, (__bridge void*)self);
    
    [self readNextFile];
}

- (void)dealloc
{
    if (mDisplayLink)
    {
        if (CVDisplayLinkIsRunning(mDisplayLink))
        {
            CVDisplayLinkStop(mDisplayLink);
        }
        CVDisplayLinkRelease(mDisplayLink);
    }
}

- (void)didAddFrame
{
    const int64_t framesIn = OSAtomicIncrement32(&self->mNumFramesInBuffer);
    const char* func = __PRETTY_FUNCTION__;
    dispatch_async(mLogQueue, ^{  NSLog(@"%s frameCount: %@", func, @(framesIn)); });
    if (framesIn == kMaxFramesInPipelineAtOnce) // we've transitioned to "at the limit"
    {
        dispatch_suspend(mFrameReadQueue);
    }
}

- (void)didRemoveFrame
{
    const int64_t framesIn = OSAtomicDecrement32(&self->mNumFramesInBuffer);
    const char* func = __PRETTY_FUNCTION__;
    dispatch_async(mLogQueue, ^{  NSLog(@"%s frameCount: %@", func, @(framesIn)); });
    if (framesIn == kMaxFramesInPipelineAtOnce - 1) // we transitioned to "under the limit"
    {
        dispatch_resume(mFrameReadQueue);
    }
}

- (void)readNextFile
{
    dispatch_async (mFrameReadQueue, ^{
        NSURL* url = [[NSBundle mainBundle] URLForResource: [NSString stringWithFormat: @"File%lu", mFileIndex++] withExtension: @"txt"];
        
        if (!url)
        {
            // Run out the buffer with dummy frames;
            dispatch_data_t empty = dispatch_data_create(NULL, 0, NULL, NULL);
            for (NSUInteger i = 0; i < kFramesToOverlap; i++)
            {
                // we have to dispatch, and then dispatch_after here to preserve the order, otherwise the fake frames could jump the line...
                dispatch_async(mFrameReadQueue, ^{
                    double delayInSeconds = 0.012;
                    dispatch_time_t popTime = dispatch_time(DISPATCH_TIME_NOW, (int64_t)(delayInSeconds * NSEC_PER_SEC));
                    dispatch_after(popTime, mFrameReadQueue, ^(void){
                        [self didAddFrame];
                        [self decodeFrameData: empty fromFile: (id)[NSNull null]];
                    });
                });
            }
            
            return;
        }
        
        // We don't care what queue the cleanup handler gets called on, because we know there's only ever one file being read at a time
        dispatch_io_t readChannel = dispatch_io_create_with_path(DISPATCH_IO_STREAM, [[url path] fileSystemRepresentation], O_RDONLY|O_NONBLOCK, 0, mFrameReadQueue, ^(int error) {
            DieOnError(error);
            
            mFrameReadAccumulator = nil;
            
            // Start the next file
            [self readNextFile];
        });
        
        // We don't care what queue the read handlers get called on, because we know they're inherently serial
        dispatch_io_read(readChannel, 0, SIZE_MAX, mFrameReadQueue, ^(bool done, dispatch_data_t data, int error) {
            DieOnError(error);
            
            // Grab frames
            dispatch_data_t localAccumulator = mFrameReadAccumulator ? dispatch_data_create_concat(mFrameReadAccumulator, data) : data;
            if (done)
            {
                // In case the trailing newline is missing...
                localAccumulator = dispatch_data_create_concat(localAccumulator, dispatch_data_create("\n", 1, NULL, DISPATCH_DATA_DESTRUCTOR_DEFAULT));
            }
            dispatch_data_t frameData = nil;
            do
            {
                frameData = FrameDataFromAccumulator(&localAccumulator);
                mFrameReadAccumulator = localAccumulator;
                // mimic 12ms of reading time
                if (frameData && dispatch_data_get_size(frameData) > 1)
                {
                    double delayInSeconds = 0.012;
                    dispatch_time_t popTime = dispatch_time(DISPATCH_TIME_NOW, (int64_t)(delayInSeconds * NSEC_PER_SEC));
                    dispatch_after(popTime, mFrameReadQueue, ^(void){
                        [self didAddFrame];
                        [self decodeFrameData: frameData fromFile: url];
                    });
                }
            } while (frameData);
            
            if (done)
            {
                dispatch_io_close(readChannel, DISPATCH_IO_STOP);
            }
        });
    });
}


- (void)decodeFrameData: (dispatch_data_t)frameData fromFile: (NSURL*)file
{
    __block dispatch_data_t decodedData = nil;
    EnqueueOrderedParallelizableWork(^{
        // Do the work - 13ms +/- 2.5ms
        int random = (rand() % 5000) - 2500 + 13000;
        usleep(random);
        NSString* decodedFrame = [NSStringFromDispatchData(frameData) uppercaseString];
        decodedData = dispatch_data_create(decodedFrame.UTF8String, decodedFrame.length, NULL, DISPATCH_DATA_DESTRUCTOR_DEFAULT);
    }, ^{
        [self processFrameData:decodedData fromFile:file];
    }, mFrameDataProcessingQueue);
}

- (void)processFrameData: (dispatch_data_t)frameData fromFile: (NSURL*)file
{
    if (!frameData || !file)
        return;
    
    // We want the data blobs constituting each frame to be processed serially
    dispatch_async(mFrameDataProcessingQueue, ^{
        mFilesForOverlapping = mFilesForOverlapping ?: [NSMutableArray array];
        mFrameArraysForOverlapping = mFrameArraysForOverlapping ?: [NSMutableArray array];
        
        NSMutableArray* arrayToAddTo = nil;
        if ([file isEqual: mFilesForOverlapping.lastObject])
        {
            arrayToAddTo = mFrameArraysForOverlapping.lastObject;
        }
        else
        {
            arrayToAddTo = [NSMutableArray array];
            [mFilesForOverlapping addObject: file];
            [mFrameArraysForOverlapping addObject: arrayToAddTo];
        }
        
        [arrayToAddTo addObject: frameData];
        
        // We've gotten to file two, and we have enough frames to process the overlap
        if (mFrameArraysForOverlapping.count == 2 && [mFrameArraysForOverlapping[1] count] >= kFramesToOverlap)
        {
            NSMutableArray* fileOneFrames = mFrameArraysForOverlapping[0];
            NSMutableArray* fileTwoFrames = mFrameArraysForOverlapping[1];
            
            for (NSUInteger i = 0; i < kFramesToOverlap; ++i)
            {
                [self blendOneFrame:fileOneFrames[0] withOtherFrame: fileTwoFrames[0]];
                [fileOneFrames removeObjectAtIndex:0];
                [fileTwoFrames removeObjectAtIndex:0];
            }
            
            [mFilesForOverlapping removeObjectAtIndex: 0];
            [mFrameArraysForOverlapping removeObjectAtIndex: 0];
        }
        
        // We're pulling in frames from file 1, haven't gotten to file 2 yet, have more than enough to overlap
        while (mFrameArraysForOverlapping.count == 1 && [mFrameArraysForOverlapping[0] count] > kFramesToOverlap)
        {
            NSMutableArray* frameArray = mFrameArraysForOverlapping[0];
            dispatch_data_t first = frameArray[0];
            [mFrameArraysForOverlapping[0] removeObjectAtIndex: 0];
            [self blendOneFrame: first withOtherFrame: nil];
        }
    });
}

- (void)blendOneFrame: (dispatch_data_t)frameA withOtherFrame: (dispatch_data_t)frameB
{
    dispatch_async(mFrameBlendingQueue, ^{
        NSString* blendedFrame = [NSString stringWithFormat: @"%@%@", [NSStringFromDispatchData(frameA) stringByReplacingOccurrencesOfString: @"\n" withString:@""], NSStringFromDispatchData(frameB)];
        dispatch_data_t blendedFrameData = dispatch_data_create(blendedFrame.UTF8String, blendedFrame.length, NULL, DISPATCH_DATA_DESTRUCTOR_DEFAULT);
        [self deliverFrameForDisplay: blendedFrameData];
        if (frameA && frameB)
        {
            [self didRemoveFrame]; // 2 frames becomes 1, that's removing a frame
        }
    });
}

- (void)deliverFrameForDisplay: (dispatch_data_t)frame
{
    // By suspending the queue from within the block, and by virtue of this being a serial queue, we guarantee that
    // only one task will get called for each call to dispatch_resume on the queue...
    
    dispatch_async(mFrameDeliveryQueue, ^{
        dispatch_suspend(mFrameDeliveryQueue);
        dispatch_sync(mFrameDeliveryStateQueue, ^{
            mLastFrameDelivered++;
            mDeliveredFrame = frame;
        });
        
        if (!CVDisplayLinkIsRunning(mDisplayLink))
        {
            CVDisplayLinkStart(mDisplayLink);
        }
    });
}

- (dispatch_data_t)getFrameForDisplay
{
    __block dispatch_data_t frameData = nil;
    dispatch_sync(mFrameDeliveryStateQueue, ^{
        if (mLastFrameDelivered > mLastFrameDisplayed)
        {
            frameData = mDeliveredFrame;
            mDeliveredFrame = nil;
            mLastFrameDisplayed = mLastFrameDelivered;
            [self didRemoveFrame];
        }
    });

    // At this point, I've either got the next frame or I dont...
    // resume the delivery queue so it will deliver the next frame
    if (frameData)
    {
        dispatch_resume(mFrameDeliveryQueue);
    }

    return frameData;
}

static CVReturn MyDisplayLinkCallback(CVDisplayLinkRef displayLink, const CVTimeStamp* now, const CVTimeStamp* outputTime, CVOptionFlags flagsIn, CVOptionFlags* flagsOut, void* displayLinkContext)
{
    SOAppDelegate* self = (__bridge SOAppDelegate*)displayLinkContext;
    
    dispatch_data_t frameData = [self getFrameForDisplay];
    
    NSString* dataAsString = NSStringFromDispatchData(frameData);
    
    if (dataAsString.length == 0)
    {
        dispatch_async(self->mLogQueue, ^{ NSLog(@"Dropped frame..."); });
    }
    else
    {
        dispatch_async(self->mLogQueue, ^{  NSLog(@"Drawing frame in CVDisplayLink. Contents: %@", dataAsString); });
    }
    
    return kCVReturnSuccess;
}

@end

static void DieOnError(int error)
{
    if (error)
    {
        NSLog(@"Error in %s: %s", __PRETTY_FUNCTION__, strerror(error));
        exit(error);
    }
}

static NSString* NSStringFromDispatchData(dispatch_data_t data)
{
    if (!data || !dispatch_data_get_size(data))
        return @"";
    
    const char* buf = NULL;
    size_t size = 0;
    dispatch_data_t notUsed = dispatch_data_create_map(data, (const void**)&buf, &size);
#pragma unused(notUsed)
    NSString* str = [[NSString alloc] initWithBytes: buf length: size encoding: NSUTF8StringEncoding];
    return str;
}

// Peel off a frame if there is one, and put the left-overs back.
static dispatch_data_t FrameDataFromAccumulator(dispatch_data_t* accumulator)
{
    __block dispatch_data_t frameData = dispatch_data_create(NULL, 0, NULL, NULL); // empty
    __block dispatch_data_t leftOver = dispatch_data_create(NULL, 0, NULL, NULL); // empty
    
    __block BOOL didFindFrame = NO;
    dispatch_data_apply(*accumulator, ^bool(dispatch_data_t region, size_t offset, const void *buffer, size_t size) {
        ssize_t newline = -1;
        for (size_t i = 0; !didFindFrame && i < size; ++i)
        {
            if (((const char *)buffer)[i] == '\n')
            {
                newline = i;
                break;
            }
        }
        
        if (newline == -1)
        {
            if (!didFindFrame)
            {
                frameData = dispatch_data_create_concat(frameData, dispatch_data_create(buffer, size, NULL, DISPATCH_DATA_DESTRUCTOR_DEFAULT));
            }
            else
            {
                leftOver = dispatch_data_create_concat(leftOver, dispatch_data_create(buffer, size, NULL, DISPATCH_DATA_DESTRUCTOR_DEFAULT));
            }
        }
        else if (newline >= 0)
        {
            didFindFrame = YES;
            frameData = dispatch_data_create_concat(frameData, dispatch_data_create(buffer, newline + 1, NULL, DISPATCH_DATA_DESTRUCTOR_DEFAULT));// ideally I'd do dispatch_data_create_subrange(region, 0, newline + 1)); here but I've noticed some issues with that.
            leftOver = dispatch_data_create_concat(leftOver, dispatch_data_create(((uint8_t*)buffer) + newline + 1, size - newline - 1, NULL, DISPATCH_DATA_DESTRUCTOR_DEFAULT));
            return false;
        }
        
        return true;
    });
    
    if (!didFindFrame)
    {
        leftOver = dispatch_data_create_concat(frameData, leftOver);
    }
    
    frameData = didFindFrame && dispatch_data_get_size(frameData) > 1 ? frameData : nil;
    
    *accumulator = leftOver;
    
    return frameData;
}

static void EnqueueOrderedParallelizableWork(dispatch_block_t workBlock, dispatch_block_t completionBlock, dispatch_queue_t completionQueue)
{
    // Protect our pending items state
    static dispatch_queue_t sSerialQueue;
    static NSMutableArray* sPendingItems;
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        sSerialQueue = dispatch_queue_create("", DISPATCH_QUEUE_SERIAL);
        sPendingItems = [[NSMutableArray alloc] init];
    });
    
    // I'm too lazy to make a full subclass for this stuff -- associated sotrage to the rescue!
    static void * const WorkItemCompletionBlockKey = (void*)&WorkItemCompletionBlockKey;
    static void * const WorkItemCompletionQueueKey = (void*)&WorkItemCompletionQueueKey;
    static void * const WorkItemTaskCompleteKey = (void*)&WorkItemTaskCompleteKey;

    // Build the work item
    NSObject* workItem = [[NSObject alloc] init];
    objc_setAssociatedObject(workItem, WorkItemCompletionBlockKey, completionBlock, OBJC_ASSOCIATION_RETAIN);
    objc_setAssociatedObject(workItem, WorkItemCompletionQueueKey, completionQueue, OBJC_ASSOCIATION_RETAIN);
    
    // Dispatch it.
    dispatch_async(sSerialQueue, ^{
        // Add it to the list
        [sPendingItems insertObject: workItem atIndex: 0];

        // Use a group to get a callback
        dispatch_group_t group = dispatch_group_create();
        
        // Now kick off the real work
        dispatch_group_async(group, dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), workBlock);
        
        // Call us back when that work completes
        dispatch_group_notify(group, sSerialQueue, ^{
            // Mark it finished...
            objc_setAssociatedObject(workItem, WorkItemTaskCompleteKey, (id)@(YES), OBJC_ASSOCIATION_ASSIGN);
            
            // Fire completions for any completed tasks at the end of the queue.
            for (NSObject* completedWorkItem = sPendingItems.lastObject; objc_getAssociatedObject(completedWorkItem, WorkItemTaskCompleteKey); [sPendingItems removeLastObject], completedWorkItem = sPendingItems.lastObject)
            {
                dispatch_queue_t innerCompletionQueue = objc_getAssociatedObject(completedWorkItem, WorkItemCompletionQueueKey);
                dispatch_block_t innerCompletionBlock = objc_getAssociatedObject(completedWorkItem, WorkItemCompletionBlockKey);
                dispatch_async(innerCompletionQueue, innerCompletionBlock);
            }
        });
    });
}





















