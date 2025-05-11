const std = @import("std");
const Allocator = std.mem.Allocator;
// page 0 is the header page
// digits until \0 is the number of pages

// want i want for a v0 is
//
const PAGE_SIZE = 4096;
const GOD_MODE: std.posix.mode_t = 0o666;

fn Pager(nPages: u16) type {
    return struct {
        const Node = struct {
            pageNumber: u32,
            offset: u32,
            // use this to
            next: ?u8,
            dirty: bool,
        };

        const Self = @This();

        nCurrentlyCached: u16,
        cachedPagesHead: ?Node,
        cachedPages: [nPages]Node = undefined,
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
            if (self.tail < self.head) {
                // can scan from left to right
                for (self.tail..self.head) |i| {
                    if (self.cachedPages[i].pageNumber == pageNumber) {
                        return self.cachedPages[i].offset;
                    }
                }
            }

            if (self.tail > self.head) {
                // need to scan from 0 to head and tail to nPages
                for (0..self.head) |i| {
                    if (self.cachedPages[i].pageNumber == pageNumber) {
                        return self.cachedPages[i].offset;
                    }
                }
                for (self.tail..nPages) |i| {
                    if (self.cachedPages[i].pageNumber == pageNumber) {
                        return self.cachedPages[i].offset;
                    }
                }
            }

            return null;
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
test "pager eviction" {
    const backing_buf = try std.testing.allocator.alloc(u8, PAGE_SIZE * 10);
    defer std.testing.allocator.free(backing_buf);

    var pager = try Pager(3).init("test-out/test3", backing_buf);
    defer pager.deinit();

    // expecting page five to be flushed since it was loaded into the cache first
    var pageFive = try pager.getPage(5);
    var pageZero = try pager.getPage(0);

    for (0..pageZero.len) |i| {
        pageZero[i] = 0;
        pageFive[i] = 5;
    }

    _ = try pager.getPage(1);
}
