//
//  SOAppDelegate.m
//  ProducerConsumer
//
//  Created by Ian McCullough on 8/20/13.
//  Copyright (c) 2013 Ian McCullough. All rights reserved.
//

#import "SOAppDelegate.h"

static void DieOnError(int error);
static NSString* NSStringFromDispatchData(dispatch_data_t data);
static dispatch_data_t FrameDataFromAccumulator(dispatch_data_t* accumulator);
static CVReturn MyDisplayLinkCallback(CVDisplayLinkRef displayLink, const CVTimeStamp* now, const CVTimeStamp* outputTime, CVOptionFlags flagsIn, CVOptionFlags* flagsOut, void* displayLinkContext);

static const NSUInteger kFramesToOverlap = 15;

@implementation SOAppDelegate
{
    // Display link state
    CVDisplayLinkRef mDisplayLink;
    
    // State for our file reading process -- protected via mFrameReadQueue
    dispatch_queue_t mFrameReadQueue;
    NSUInteger mFileIndex; // keep track of what file we're reading
    dispatch_io_t mReadingChannel; // channel for reading
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
    
    CVDisplayLinkCreateWithActiveCGDisplays(&mDisplayLink);
    CVDisplayLinkSetOutputCallback(mDisplayLink, &MyDisplayLinkCallback, (__bridge void*)self);
    
    [self startPlaying];
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

- (void)startPlaying
{
    NSURL* url = [self nextFileToRead];
    if (url)
    {
        [self readFile: url];
    }
}

- (NSURL*)nextFileToRead
{
    NSURL* url = [[NSBundle mainBundle] URLForResource: [NSString stringWithFormat: @"File%lu", mFileIndex++] withExtension: @"txt"];
    return url;
}

- (void)readFile: (NSURL*)url
{
    if (mReadingChannel)
    {
        dispatch_io_close(mReadingChannel, DISPATCH_IO_STOP);
        mReadingChannel = nil;
    }
    
    // We don't care what queue the cleanup handler gets called on, because we know there's only ever one file being read at a time
    mReadingChannel = dispatch_io_create_with_path(DISPATCH_IO_STREAM, [[url path] fileSystemRepresentation], O_RDONLY|O_NONBLOCK, 0, mFrameReadQueue, ^(int error) {
        DieOnError(error);
        
        mReadingChannel = nil;
        
        // Start the next file
        NSURL* url = [self nextFileToRead];
        if (url)
        {
            [self readFile: url];
        }
    });
    
    // We don't care what queue the read handlers get called on, because we know they're inherently serial
    dispatch_io_read(mReadingChannel, 0, SIZE_MAX, mFrameReadQueue, ^(bool done, dispatch_data_t data, int error) {
        DieOnError(error);
        
        // Grab frames
        dispatch_data_t localAccumulator = mFrameReadAccumulator ? dispatch_data_create_concat(mFrameReadAccumulator, data) : data;
        dispatch_data_t frameData = nil;
        do
        {
            frameData = FrameDataFromAccumulator(&localAccumulator);
            mFrameReadAccumulator = localAccumulator;
            [self processFrameData: frameData fromFile: url];
        } while (frameData);
        
        if (done)
        {
            dispatch_io_close(mReadingChannel, DISPATCH_IO_STOP);
        }
    });
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
        NSLog(@"Dropped frame...");
    }
    else
    {
        NSLog(@"Drawing frame in CVDisplayLink. Contents: %@", dataAsString);
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
                frameData = dispatch_data_create_concat(frameData, region);
            }
            else
            {
                leftOver = dispatch_data_create_concat(leftOver, region);
            }
        }
        else if (newline >= 0)
        {
            didFindFrame = YES;
            frameData = dispatch_data_create_concat(frameData, dispatch_data_create_subrange(region, 0, newline + 1));
            leftOver = dispatch_data_create_concat(leftOver, dispatch_data_create_subrange(region, newline + 1, size - newline - 1));
        }
        
        return true;
    });
    
    *accumulator = leftOver;
    
    return didFindFrame ? frameData : nil;
}
