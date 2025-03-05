const std = @import("std");

fn BlockingRingBufferQueue(comptime T: type, comptime buff_size: u16) type {
    return struct {
        const Self = @This();
        buf: [buff_size]T = undefined,
        head: u16 = 0,
        tail: u16 = 0,
        mtx: std.Thread.Mutex = std.Thread.Mutex{},
        not_empty: std.Thread.Condition = std.Thread.Condition{},
        not_full: std.Thread.Condition = std.Thread.Condition{},

        pub fn push(self: *Self, data: T) void {
            self.mtx.lock();

            while(self.full()) {
                self.not_full.wait(&self.mtx);
            }

            self.buf[self.head] = data;
            const should_signal_not_empty = self.empty();
            self.head = (self.head + 1) % buff_size;

            self.mtx.unlock();

            if(should_signal_not_empty) {
                self.not_empty.signal();
            }
        }

        // caller is assumed to hold the lock
        fn empty(self: *const Self) bool {
            return self.head == self.tail;
        }
        pub fn pop(self: *Self) T {
            self.mtx.lock();

            while(self.empty()) {
                self.not_empty.wait(&self.mtx);
            }
             
            const result = self.buf[self.tail];
            const should_signal_not_full = self.full();
            self.tail = (self.tail + 1) % buff_size;

            self.mtx.unlock();

            if(should_signal_not_full) {
                self.not_full.signal();
            }

            return result;
        }

        pub fn try_pop(self: *Self) ?T {
            self.mtx.lock();

            const result: ?T = undefined;
            const should_signal_not_full: bool = undefined;
            if(!self.empty()) {
                result = self.buf[self.tail];
                should_signal_not_full = self.full();
                self.tail = (self.tail + 1) % buff_size;
            } else {
                result = null;
                should_signal_not_full = false;
            }

            self.mtx.unlock();
            if(should_signal_not_full) {
                self.not_full.signal();
            }

            return result;
        }

        // caller is assumed to hold the lock
        fn full(self: *const Self) bool {
            return (self.head + 1) % buff_size == self.tail;
        }

    };
}

const expectEqual = std.testing.expectEqual;
const expect = std.testing.expect;

test "basic single threaded push and pop" {
    var queue = BlockingRingBufferQueue(usize, 10){};
    queue.push(42);
    queue.push(69);
    queue.push(420);

    try expectEqual(queue.pop(), 42);
    try expectEqual(queue.pop(), 69);
    try expectEqual(queue.pop(), 420);
}

test "two threads" {
    var queue = BlockingRingBufferQueue(usize, 10){};
    const size: usize = 10000;

    const task_one = struct {
        fn run(q: *BlockingRingBufferQueue(usize, 10)) void {
            for(0..size) |i| {
                q.push(i);
            }
        }
    };

    const task_two = struct {
        fn run(q: *BlockingRingBufferQueue(usize, 10), failed: *bool) void {
            var seen_before: [size]bool = undefined;
            for(0..size) |i| {
                seen_before[i] = false;
            }

            for(0..size) |_| {
                const result = q.pop();
                if(expect(!seen_before[result])) |_| {} else |_| {
                    failed.* = true;
                }
                seen_before[result] = true;
            }
        }
    };

    var failed = false;
    var t1 = try std.Thread.spawn(.{}, task_one.run, .{&queue});
    var t2 = try std.Thread.spawn(.{}, task_two.run, .{&queue, &failed});
    
    try expect(!failed);

    t1.join();
    t2.join();
}
