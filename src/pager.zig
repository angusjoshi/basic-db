const std = @import("std");
const Allocator = std.mem.Allocator;

const PAGE_SIZE = 4096;
const GOD_MODE: std.posix.mode_t = 0o666;

const assert = std.debug.assert;

// a statically allocated LRU cache with linear scans of a small buffer for finds
// pages are automatically flushed when evicted
// TODO store a dirty flag and flush on evict only if dirty
// i think my interface is bad tbh. i want to,
//      - allow callers to decide when to flush
//      - make the returned buffer from getPage somewhat safe (at the moment it's pretty broken if the page is evicted)
//      - flush on eviction only if the page is dirty (not sure what dirty means here yet. caller marks it as dirty?)
//      - the option to pin a page is a nice to have
// problems at the moment are,
//      - the buffer returned by getPage can have the backing page evicted,
//      - pages are always flushed on eviction,
fn Pager(comptime nPages: u8) type {
    return struct {
        const Self = @This();
        const PageNumber = u32;

        const Node = struct {
            next: ?u8,
            prev: ?u8,
        };

        size: u8,
        head: u8,
        tail: u8,
        // store the page numbers out of band with the nodes for faster linear scans
        nodes: [nPages]Node,
        pages: [nPages]PageNumber,

        backingBuf: []u8,
        fd: std.posix.fd_t,

        // returns the index in the cache and if a page was evicted, its page number.
        fn insert(self: *Self, pageNumber: u32) struct { u8, ?u32 } {
            if (self.size == 0) {
                self.nodes[0] = .{
                    .next = null,
                    .prev = null,
                };
                self.pages[0] = pageNumber;

                self.head = 0;
                self.tail = 0;
                self.size += 1;

                std.debug.assert(self.isWellFormed());
                return .{ 0, null };
            }

            if (self.size < nPages) {
                assert(self.size >= 1);
                self.nodes[self.size] = .{
                    .prev = self.size - 1,
                    .next = null,
                };
                self.pages[self.size] = pageNumber;

                self.nodes[self.tail].next = self.size;
                self.nodes[self.size].prev = self.tail;
                self.tail = self.size;
                self.size += 1;

                assert(self.isWellFormed());
                return .{ self.size - 1, null };
            }

            // cache is full; evict the head (least recently used).
            assert(self.nodes[self.head].prev == null);
            const pageBeingEvicted = self.pages[self.head];
            self.pages[self.head] = pageNumber;

            return .{ self.head, pageBeingEvicted };
        }

        fn findIdx(self: *Self, pageNumber: u32) ?u8 {
            for (0..self.size) |i| {
                if (self.pages[i] == pageNumber) {
                    return @intCast(i);
                }
            }

            return null;
        }

        fn findWithoutTouch(self: *Self, pageNumber: u32) ?u8 {
            if (self.findIdx(pageNumber)) |idx| {
                return idx;
            }

            return null;
        }

        fn isWellFormed(self: *Self) bool {
            if (self.size > 0) {
                return self.nodes[self.head].prev == null and
                    self.nodes[self.tail].next == null;
            }

            return true;
        }
        fn findWithTouch(self: *Self, pageNumber: u32) ?u8 {
            if (self.findIdx(pageNumber)) |idx| {
                assert(self.isWellFormed());
                if (idx == self.tail) {
                    assert(self.isWellFormed());
                    return idx;
                }
                if (idx == self.head) {
                    const newHead = self.nodes[idx].next.?;
                    self.nodes[self.tail].next = idx;
                    self.nodes[idx].prev = self.tail;
                    self.nodes[idx].next = null;
                    self.head = newHead;
                    self.tail = idx;
                    self.nodes[newHead].prev = null;

                    assert(self.isWellFormed());
                    return idx;
                }

                assert(self.nodes[idx].next != null);
                assert(self.nodes[idx].prev != null);

                if (self.nodes[idx].prev) |prev| {
                    self.nodes[prev].next = self.nodes[idx].next;
                }
                if (self.nodes[idx].next) |next| {
                    self.nodes[next].prev = self.nodes[idx].prev;
                }

                self.nodes[self.tail].next = idx;
                self.nodes[idx].prev = self.tail;
                self.nodes[idx].next = null;
                self.tail = idx;

                assert(self.isWellFormed());
                return idx;
            }

            return null;
        }

        pub fn getPage(self: *Self, pageNumber: u32) ![]u8 {
            if (self.findPageOffsetInCache(pageNumber)) |offset| {
                return self.backingBuf[offset..(offset + PAGE_SIZE)];
            }

            return self.loadIntoCache(pageNumber);
        }

        fn flushPageIdx(self: *Self, idx: u8, pageNumber: u32) !void {
            const offset: u32 = PAGE_SIZE * @as(u32, idx);
            try std.posix.lseek_SET(self.fd, pageNumber * PAGE_SIZE);
            _ = try std.posix.write(self.fd, self.backingBuf[offset..(offset + PAGE_SIZE)]);
        }

        pub fn flushPage(self: *Self, pageNumber: u32) !void {
            if (self.findWithoutTouch(pageNumber)) |idx| {
                try self.flushPageIdx(idx, pageNumber);
            }
        }

        fn loadIntoCache(self: *Self, pageNumber: u32) ![]u8 {
            const idx, const evictedPageNumber = self.insert(pageNumber);
            if (evictedPageNumber) |page| {
                try self.flushPageIdx(idx, page);
            }

            try std.posix.lseek_SET(self.fd, pageNumber * PAGE_SIZE);
            const resultBuf = self.backingBuf[(@as(u32, idx) * PAGE_SIZE)..(@as(u32, idx + 1) * PAGE_SIZE)];
            _ = try std.posix.read(self.fd, resultBuf);

            return resultBuf;
        }

        fn findPageOffsetInCache(self: *Self, pageNumber: u32) ?u32 {
            if (self.findWithTouch(pageNumber)) |idx| {
                return @as(u32, idx) * PAGE_SIZE;
            }

            return null;
        }

        pub fn init(filePath: []const u8, backingBuf: []u8) !Self {
            std.debug.assert(backingBuf.len % PAGE_SIZE == 0);

            const fd = try std.posix.open(filePath, .{ .ACCMODE = .RDWR, .CREAT = true }, GOD_MODE);

            return Self{
                .size = 0,
                .head = undefined,
                .tail = undefined,
                .nodes = undefined,
                .pages = undefined,
                .fd = fd,
                .backingBuf = backingBuf,
            };
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
    var backingBuf: [PAGE_SIZE * 42]u8 = undefined;
    var underTest = try Pager(5).init("test-out/test-one", &backingBuf);

    const idx, _ = underTest.insert(42);

    try std.testing.expectEqual(idx, underTest.findWithTouch(42));
}

test "more CacheList" {
    var backingBuf: [PAGE_SIZE * 42]u8 = undefined;
    var underTest = try Pager(5).init("test-out/test-one", &backingBuf);

    var indices: [6]u8 = undefined;
    for (0..6) |i| {
        indices[i], _ = underTest.insert(@intCast(i));
    }

    try std.testing.expectEqual(null, underTest.findWithoutTouch(0));
    for (1..6) |i| {
        try std.testing.expectEqual(indices[i], underTest.findWithoutTouch(@intCast(i)));
    }
}

// test "CacheList LRU" {
//     var backingBuf: [PAGE_SIZE * 42]u8 = undefined;
//     var underTest = try Pager(5).init("test-out/test-one", &backingBuf);
//
//     var indices: [6]u8 = undefined;
//     for (0..6) |i| {
//         indices[i] = underTest.insert(@intCast(i));
//     }
//
//     try std.testing.expectEqual(null, underTest.findWithoutTouch(0));
//     _ = underTest.findWithTouch(3);
//     _ = underTest.findWithTouch(2);
//     _ = underTest.findWithTouch(5);
//     _ = underTest.findWithTouch(0);
//     _ = underTest.findWithTouch(10);
//     _ = underTest.findWithTouch(4);
//     _ = underTest.findWithTouch(1);
//     _ = underTest.findWithTouch(3);
//
//     _ = underTest.insert(42);
//
//     for (0..5) |i| {
//         const iAsU8: u8 = @intCast(i);
//         try std.testing.expectEqual(iAsU8, underTest.findWithoutTouch(@intCast(i)));
//     }
//
//     try std.testing.expectEqual(null, underTest.findWithoutTouch(5));
// }
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

// // test "pager init" {
// //     var backing_buf: [2 * PAGE_SIZE]u8 = undefined;
// //
// //     var pager = try Pager(42).init("test-out/some_name", &backing_buf);
// //
// //     pager.deinit();
// // }
//
test "pager basic page" {
    const backing_buf = try std.testing.allocator.alloc(u8, PAGE_SIZE * 10);
    defer std.testing.allocator.free(backing_buf);

    var pager = try Pager(10).init("test-out/test2", backing_buf);

    var pageZero = try pager.getPage(0);
    var pageOne = try pager.getPage(1);
    var pageFive = try pager.getPage(5);

    for (0..pageZero.len) |i| {
        pageZero[i] = @intCast((i + 0) % 256);
        pageOne[i] = @intCast((i + 1) % 256);
        pageFive[i] = @intCast((i + 5) % 256);
    }

    try pager.flushPage(1);
    try pager.flushPage(5);
    try pager.flushPage(0);

    pager.deinit();

    var buf: [PAGE_SIZE]u8 = undefined;
    const fd2 = try std.posix.open("test-out/test2", .{ .ACCMODE = .RDONLY, .CREAT = true }, 0);

    // page five
    try std.posix.lseek_SET(fd2, PAGE_SIZE * 5);
    const read = try std.posix.read(fd2, &buf);

    try std.testing.expectEqual(PAGE_SIZE, read);
    for (0..PAGE_SIZE) |i| {
        const expected: u8 = @intCast((i + 5) % 256);
        try std.testing.expectEqual(expected, buf[i]);
    }

    // page one
    try std.posix.lseek_SET(fd2, PAGE_SIZE * 1);
    const alsoRead = try std.posix.read(fd2, &buf);

    try std.testing.expectEqual(PAGE_SIZE, alsoRead);
    for (0..PAGE_SIZE) |i| {
        const expected: u8 = @intCast((i + 1) % 256);
        try std.testing.expectEqual(expected, buf[i]);
    }

    // page zero
    try std.posix.lseek_SET(fd2, PAGE_SIZE * 0);
    const alsoAlsoRead = try std.posix.read(fd2, &buf);

    try std.testing.expectEqual(PAGE_SIZE, alsoAlsoRead);
    for (0..PAGE_SIZE) |i| {
        const expected: u8 = @intCast((i + 0) % 256);
        try std.testing.expectEqual(expected, buf[i]);
    }

    std.posix.close(fd2);
}
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
