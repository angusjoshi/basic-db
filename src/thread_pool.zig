const std = @import("std");
const Future = @import("future.zig").Future;
const assert = std.debug.assert;

const HARDWARE_CONCURRENCY = 10;
const Allocator = std.mem.Allocator;
const WORK_QUEUE_SIZE = 200;

fn ThreadPool(comptime A: type, comptime V: type) type {
    return struct { 
        const Self = @This();

        const Work = struct {
            work_fn: fn(A) V,
            args: A,
            ftr: Future(V),
        };

        threads: [HARDWARE_CONCURRENCY]std.Thread = undefined,
        allocator: std.mem.Allocator = undefined,
        work_queue: [WORK_QUEUE_SIZE]Work = undefined,
        work_mtx: std.Thread.Mutex = std.Thread.Mutex{},
        work_cnd: std.Thread.Condition = std.Thread.Condition{},
        head: u8 = 0,
        tail: u8 = 0,

        // put work on the work queue, or caller executes if queue is full
        fn submit(self: *Self, f: fn(args: A) V, args: A) !Future(V) {
            self.work_mtx.lock();

            if(self.work_full()) {
                // queue is full. caller executes and returns a completed future
                self.work_mtx.unlock();
                const result = f(args);
                const result_ftr = try Future(V).init();
                result_ftr.complete(result);
                return result_ftr;
            }

            self.push_work(.{f, args});
            self.work_mtx.unlock();
        }

        // caller is assumed to hold a lock on work_mtx, and the queue is assumed to be not full
        fn push_work(self: *Self, work: Work) void {
            assert(!self.work_full());
            
            self.work_queue[self.head] = work;
            self.head = (self.head + 1) % WORK_QUEUE_SIZE;
        }

        // caller is assumed to hold a lock on work_mtx
        fn work_empty(self: *Self) bool {
            return self.head == self.tail;
        }

        // called is assumed to hold a lock on work_mtx
        fn work_full(self: *Self) bool {
            return (self.head + 1) % WORK_QUEUE_SIZE == self.tail;
        }

        // caller is assumed to hold a lock on work_mtx
        // and the queue is assumed to be non-empty.
        fn pop_work(self: *Self) Work {
            assert(!self.work_empty());
            
            const result = self.work_queue[self.tail];
            self.tail = (self.tail + 1) % WORK_QUEUE_SIZE;

            return result;
        }

        fn do_work(self: *Self) void {
            while(true) {
                self.work_mtx.lock();
                while(self.work_empty()) {
                    self.work_cnd.wait(&self.work_mtx);
                }
                const work = pop_work();
                self.work_mtx.unlock();

                const result = work.work_fn(work.args);
                work.ftr.complete(result);
            }
        }

        fn init(allocator: Allocator) Self {
            var result = Self{};
            result.allocator = allocator;
            for(result.threads) |thread| {
                thread = try std.Thread.spawn(.{}, do_work, .{&result});
            }
        }

        fn deinit(self: *Self) void {
            // join threads, etc.
            for(self.threads) |*t| {
                t.join();
            }
        }
    };
}

fn the_answer(i: i32) i32 {
    _ = i;
    return 42;
}
test "one task" {
    var pool = ThreadPool(i32, i32).init(std.testing.allocator);
    const ftr = pool.submit(the_answer, 69);

    std.testing.expectEqual(ftr.get(), 42);

    ftr.deinit();
}
