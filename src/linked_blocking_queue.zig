const std = @import("std");
const assert = std.debug.assert;
fn LinkedBlockingQueue(comptime T: type) type {

    const Node = struct {
        const Self = @This();
        val: T,
        next: ?*Self,
        prev: ?*Self,
    };

    return struct{
        const Self = @This();

        head: ?*Node = null,
        tail: ?*Node = null,
        allocator: std.mem.Allocator,
        mtx: std.Thread.Mutex = std.Thread.Mutex{},
        not_empty: std.Thread.Condition = std.Thread.Condition{},

        pub fn push(self: *Self, val: T) !void {
            const new_node = try self.allocator.create(Node);
            new_node.val = val;
            new_node.prev = null;

            self.mtx.lock();
            self.not_empty.wait(&self.mtx);
                
            if(self.head) |old_head| {
                // list is not empty.
                new_node.next = old_head;
                old_head.prev = new_node;
                self.head = new_node;
                self.mtx.unlock();
                return;
            }

            // list is empty
            assert(self.tail == null);
            new_node.next = null;
            self.tail = new_node;
            self.head = new_node;
            self.mtx.unlock();
            self.not_empty.signal();
        }

        /// blocks until the the queue has an element to pop
        pub fn pop(self: *Self) T {
            // no defer unlock so we can release the lock before calling into the allocator
            self.mtx.lock();

            // head null iff queue is empty
            while(self.head == null) {
                assert(self.tail == null);
                self.not_empty.wait(&self.mtx);
            }

            // tail != null iff head != null
            assert(self.tail != null);
            const result = self.tail.?.val;
            const old_tail = self.tail;
            const new_tail = old_tail.?.prev;
            if(new_tail) |tail| {
                tail.next = null;
            } else {
                self.head = null;
            }
            self.mtx.unlock();
            self.allocator.destroy(old_tail.?);
            return result;
        }


        fn init(allocator: std.mem.Allocator) Self {
            return Self {
                .allocator = allocator,
            };
        }
    };
}

const expectEqual = std.testing.expectEqual;
test "single threaded push and pop" {
    const allocator = std.testing.allocator;
    var queue = LinkedBlockingQueue(usize).init(allocator);

    try queue.push(42);
    try queue.push(69);
    try queue.push(420);

    try expectEqual(queue.pop(), 42);
    try expectEqual(queue.pop(), 69);
    try expectEqual(queue.pop(), 420);
}
