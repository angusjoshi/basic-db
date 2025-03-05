const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn Future(comptime T: type) type {
    return struct {
        const Self = @This();

        const FutureInternal = struct {
            mtx: std.Thread.Mutex = .{},
            cnd: std.Thread.Condition = .{},
            done: bool = false,
            payload: T = undefined,
        };

        pub fn get(self: *Self) T {
            self.ftr.mtx.lock();
            defer self.ftr.mtx.unlock(); 

            while(!self.ftr.done) {
                self.ftr.cnd.wait(&self.ftr.mtx);
            }

            return self.ftr.payload;
        }

        pub fn is_done(self: *const Self) bool {
            self.ftr.mtx.lock();
            defer self.ftr.mtx.unlock();

            return self.ftr.done;
        }

        pub fn complete(self: *Self, t: T) void {
            self.ftr.mtx.lock();
            self.ftr.done = true;
            self.ftr.payload = t;
            self.ftr.mtx.unlock();

            self.ftr.cnd.signal();
        }

        ftr: *FutureInternal,
        allocator: Allocator,

        fn init(allocator: Allocator) !Future(T) {
            var ftr = try allocator.create(Future(T).FutureInternal);
            ftr.mtx = std.Thread.Mutex{};
            ftr.cnd = std.Thread.Condition{};
            ftr.done = false;
            ftr.payload = undefined;

            return Future(T) { .ftr=ftr, .allocator=allocator };
        }

        fn deinit(self: *Self) void {
            self.allocator.destroy(self.ftr);
        }
    };
}

test "future, one thread" {
    var future = try Future(i32).init(std.testing.allocator);
    future.complete(42);

    try std.testing.expectEqual(future.get(), 42);

    future.deinit();
}

fn wait_and_set(future: *Future(i32)) void {
    std.time.sleep(1000 * 1000 * 1000);
    future.complete(42);
}
test "future, waits properly" {
    var future = try Future(i32).init(std.testing.allocator);

    const thread = try std.Thread.spawn(.{}, wait_and_set, .{&future});
    thread.detach();

    try std.testing.expectEqual(future.get(), 42);

    future.deinit();
}
