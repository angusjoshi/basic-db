const std = @import("std");
const Allocator = std.mem.Allocator;
// page 0 is the header page
// digits until \0 is the number of pages

// want i want for a v0 is
//
const PAGE_SIZE = 4096;
const GOD_MODE: std.posix.mode_t = 0o666;

const assert = std.debug.assert;

fn Pager(comptime nPages: u8) type {
    return struct {
        const Node = struct {
            pageNumber: u32,
            offset: u32,
            // i think we're paying 32 bits for the bool at the end which sucks. can maybe get this down.
            next: ?u8,
            prev: ?u8,
            dirty: bool,
        };

        // a linked list (statically allocated) with LRU eviction
        // TODO this could be a hashtable. can do linear scans over just the backing array though which
        // is potentially even faster than a hashtable lookup when relatively small.
        // might need to handle the 'unitialized' case for nodes in the array for that though.
        const CacheList = struct {
            size: u8 = 0,
            head: u8 = undefined,
            tail: u8 = undefined,
            pages: [nPages]Node = undefined,

            fn insert(self: *CacheList, pageNumber: u32, offset: u32) void {
                // TODO handle same pageNumber being inserted twice?
                // could just assumed that caller is smart (i.e. will call find first)
                if (self.size == 0) {
                    self.pages[0] = .{
                        .pageNumber = pageNumber,
                        .offset = offset,
                        .next = null,
                        .prev = null,
                        .dirty = false,
                    };
                    self.head = 0;
                    self.tail = 0;
                    self.size += 1;

                    std.debug.assert(self.isWellFormed());
                    return;
                }

                if (self.size < nPages) {
                    assert(self.size >= 1);
                    self.pages[self.size] = .{
                        .pageNumber = pageNumber,
                        .offset = offset,
                        .prev = self.size - 1,
                        .next = null,
                        .dirty = false,
                    };

                    self.pages[self.tail].next = self.size;
                    self.pages[self.size].prev = self.tail;
                    self.tail = self.size;
                    self.size += 1;

                    assert(self.isWellFormed());
                    return;
                }

                // cache is full; evict the head (least recently used).
                assert(self.pages[self.head].prev == null);
                self.pages[self.head].pageNumber = pageNumber;
                self.pages[self.head].offset = offset;
                // it's probably a problem here that dirty heads need to be flushed.
                // might make sense to just inline this whole queue data structure into the Pager defn.
                // there is maybe an alterntive where the cache list does not hold the dirty flag,
                // and it is all handled by the caller. i don't think that works though because of eviction.
                self.pages[self.head].dirty = false;
            }

            fn findIdx(self: *CacheList, pageNumber: u32) ?u8 {
                var cur: ?u8 = self.head;
                while (cur != null) {
                    if (self.pages[cur.?].pageNumber == pageNumber) {
                        return cur;
                    }

                    cur = self.pages[cur.?].next;
                }

                return null;
            }

            fn findWithoutTouch(self: *CacheList, pageNumber: u32) ?u32 {
                if (self.findIdx(pageNumber)) |idx| {
                    return self.pages[idx].offset;
                }

                return null;
            }

            fn isWellFormed(self: *CacheList) bool {
                if (self.size > 0) {
                    return self.pages[self.head].prev == null and
                        self.pages[self.tail].next == null;
                }

                return true;
            }

            fn findWithTouch(self: *CacheList, pageNumber: u32) ?u32 {
                if (self.findIdx(pageNumber)) |idx| {
                    assert(self.isWellFormed());

                    if (idx == self.tail) {
                        assert(self.isWellFormed());
                        return self.pages[idx].offset;
                    }

                    if (idx == self.head) {
                        const newHead = self.pages[idx].next.?;
                        self.pages[self.tail].next = idx;
                        self.pages[idx].prev = self.tail;
                        self.pages[idx].next = null;
                        self.head = newHead;
                        self.tail = idx;
                        self.pages[newHead].prev = null;

                        assert(self.isWellFormed());
                        return self.pages[idx].offset;
                    }

                    assert(self.pages[idx].next != null);
                    assert(self.pages[idx].prev != null);

                    if (self.pages[idx].prev) |prev| {
                        self.pages[prev].next = self.pages[idx].next;
                    }
                    if (self.pages[idx].next) |next| {
                        self.pages[next].prev = self.pages[idx].prev;
                    }

                    self.pages[self.tail].next = idx;
                    self.pages[idx].prev = self.tail;
                    self.pages[idx].next = null;
                    self.tail = idx;

                    assert(self.isWellFormed());
                    return self.pages[idx].offset;
                }

                return null;
            }
        };

        const Self = @This();

        cacheList: CacheList,
        backingBuf: []u8,
        fd: std.posix.fd_t,

        pub fn getPage(self: *Self, pageNumber: u32) ![]u8 {
            if (self.findPageOffsetInCache(pageNumber)) |offset| {
                std.debug.print("here, pageNumber: {}\n", .{pageNumber});
                return self.backingBuf[offset..(offset + PAGE_SIZE)];
            }

            return self.loadIntoCache(pageNumber);
        }

        pub fn flushPage(self: *Self, pageNumber: u32) !void {
            std.debug.print("flushing {}...\n", .{pageNumber});
            if (self.findPageOffsetInCache(pageNumber)) |offset| {
                std.debug.print("got to here...\n", .{});
                _ = try std.posix.write(self.fd, self.backingBuf[offset..(offset + PAGE_SIZE)]);
            }
        }

        fn loadIntoCache(self: *Self, pageNumber: u32) ![]u8 {
            if ((self.head + 1) % nPages == self.tail) {
                // TODO better eviction policy. this is FIFO.
                // (and we have to flush when we evict because we don't know if it is dirty)
                // something like a dirty flag in Node would help.

                const toEvict = self.cachedPages[self.tail];
                try self.flushPage(toEvict.pageNumber);
                self.tail = (self.tail + 1) % nPages;
            }

            try std.posix.lseek_SET(self.fd, pageNumber * PAGE_SIZE);
            const resultBuf = self.backingBuf[(self.head * PAGE_SIZE)..((self.head + 1) * PAGE_SIZE)];
            _ = try std.posix.read(self.fd, resultBuf);

            std.debug.assert((self.head + 1) % nPages != self.tail);

            self.cachedPages[self.head] = .{ .pageNumber = pageNumber, .offset = self.head * PAGE_SIZE };

            self.head = (self.head + 1) % nPages;

            return resultBuf;
        }

        fn findPageOffsetInCache(self: *Self, pageNumber: u32) ?u32 {
            return self.cacheList.findWithTouch(pageNumber);
        }

        pub fn init(filePath: []const u8, backingBuf: []u8) !Self {
            std.debug.assert(backingBuf.len % PAGE_SIZE == 0);

            const fd = try std.posix.open(filePath, .{ .ACCMODE = .RDWR, .CREAT = true }, GOD_MODE);

            return Self{ .fd = fd, .backingBuf = backingBuf };
        }

        fn deinit(self: *Self) void {
            std.posix.close(self.fd);
        }
    };
}

test "refs" {
    std.testing.refAllDeclsRecursive(Pager(42));
}

test "CacheList works" {
    var underTest: Pager(5).CacheList = .{};
    underTest.insert(42, 42);
    try std.testing.expectEqual(42, underTest.findWithTouch(42));
}

test "more CacheList" {
    var underTest: Pager(5).CacheList = .{};
    for (0..6) |i| {
        underTest.insert(@intCast(i), @intCast(i));
    }

    try std.testing.expectEqual(null, underTest.findWithoutTouch(0));
    for (1..6) |i| {
        const iAsU32: u32 = @intCast(i);
        try std.testing.expectEqual(iAsU32, underTest.findWithoutTouch(@intCast(i)));
    }
}

test "CacheList LRU" {
    var underTest: Pager(5).CacheList = .{};
    for (0..6) |i| {
        underTest.insert(@intCast(i), @intCast(i));
    }

    try std.testing.expectEqual(null, underTest.findWithoutTouch(0));
    _ = underTest.findWithTouch(3);
    _ = underTest.findWithTouch(2);
    _ = underTest.findWithTouch(5);
    _ = underTest.findWithTouch(0);
    _ = underTest.findWithTouch(10);
    _ = underTest.findWithTouch(4);
    _ = underTest.findWithTouch(1);
    _ = underTest.findWithTouch(3);

    underTest.insert(42, 42);

    for (0..5) |i| {
        const iAsU32: u32 = @intCast(i);
        try std.testing.expectEqual(iAsU32, underTest.findWithoutTouch(@intCast(i)));
    }

    try std.testing.expectEqual(null, underTest.findWithoutTouch(5));
}
// test "files work" {
//     const fd = try std.posix.open("test-out/hehe", .{ .ACCMODE = .WRONLY, .CREAT = true }, GOD_MODE);
//     const written = try std.posix.write(fd, "Hello, World!");
//     std.posix.close(fd);
//
//     var buf: [20]u8 = undefined;
//
//     const fd2 = try std.posix.open("test-out/hehe", .{ .ACCMODE = .RDONLY, .CREAT = true }, GOD_MODE);
//     const read = try std.posix.read(fd2, &buf);
//     std.posix.close(fd2);
//
//     std.debug.print("written is: {}, read is: {}, buf is: {s}\n", .{ written, read, buf[0..read] });
// }
//
// test "pager init" {
//     var backing_buf: [2 * PAGE_SIZE]u8 = undefined;
//
//     var pager = try Pager(42).init("test-out/some_name", &backing_buf);
//
//     pager.deinit();
// }
//
// test "pager basic page" {
//     const backing_buf = try std.testing.allocator.alloc(u8, PAGE_SIZE * 10);
//     defer std.testing.allocator.free(backing_buf);
//
//     var pager = try Pager(10).init("test-out/test2", backing_buf);
//     defer pager.deinit();
//
//     var pageZero = try pager.getPage(0);
//     var pageOne = try pager.getPage(1);
//     var pageFive = try pager.getPage(5);
//
//     for (0..pageZero.len) |i| {
//         pageZero[i] = @intCast(i % 256);
//         pageOne[i] = @intCast(i % 256);
//         pageFive[i] = @intCast(i % 256);
//     }
//
//     try pager.flushPage(1);
//     try pager.flushPage(5);
//     try pager.flushPage(0);
// }
//
// test "pager eviction" {
//     const backing_buf = try std.testing.allocator.alloc(u8, PAGE_SIZE * 10);
//     defer std.testing.allocator.free(backing_buf);
//
//     var pager = try Pager(3).init("test-out/test3", backing_buf);
//     defer pager.deinit();
//
//     // expecting page five to be flushed since it was loaded into the cache first
//     var pageFive = try pager.getPage(5);
//     var pageZero = try pager.getPage(0);
//
//     for (0..pageZero.len) |i| {
//         pageZero[i] = 0;
//         pageFive[i] = 5;
//     }
//
//     _ = try pager.getPage(1);
// }
