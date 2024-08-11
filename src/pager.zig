const std = @import("std");
const io = std.io;
const Allocator = std.mem.Allocator;
// page 0 is the header page
// digits until \0 is the number of pages

const PAGE_SIZE = 4096;

const Pager = struct {
    const Self = @This();

    allocator: Allocator,
    file_path: []const u8,

    fn get_page(self: Self, offset: usize) ![]u8 {
        const file = try std.fs.cwd().openFile(self.file_path, .{});
        defer file.close();

        const buf = try self.allocator.alloc(u8, PAGE_SIZE);

        try file.seekTo(offset * PAGE_SIZE);
        _ = try file.read(buf);

        return buf;
    }
    fn write_page(self: Self, offset: usize, bytes: []const u8) !void {
        const file = try std.fs.cwd().openFile(self.file_path, .{.mode = .write_only});
        defer file.close();
        
        try file.seekTo(offset * PAGE_SIZE);
        _ = try file.write(bytes);
    }

    fn init_old_file(file_path: []const u8, allocator: Allocator) !Self {
        const file = try std.fs.cwd().createFile(file_path, .{});
        file.close();
        return Self {
            .file_path = file_path,
            .allocator = allocator,
        };
    }
    fn init_new_file(file_path: []const u8, allocator: Allocator) !Self {
        const file = try std.fs.cwd().createFile(file_path, .{.exclusive = true}); 
        const header = allocator.alloc(u8, PAGE_SIZE);
        _ = header;
        _ = file;
    }
};

test "files work" {
    const file = try std.fs.cwd().createFile("hehe", .{});
    _ = try file.write("Hello, World!");
    // file.read();
    // file.close();
}

fn test_pager() !Pager {
    const file_name: []const u8 = "wow.db";
    const pager = try Pager.init(file_name, std.testing.allocator);
    return pager;
}

test "pager works" {
    const pager = try test_pager();
    const page = try pager.get_page(0);

    std.testing.allocator.free(page);
}

test "pager read and write" {
    const pager = try test_pager();
    const bytes = std.testing.allocator.alloc([]u8, PAGE_SIZE);
    // const bytes = "hehe xd";
    const other_bytes = "nooooooooooooooooooooooo";
    try pager.write_page(0, other_bytes);
    try pager.write_page(2, bytes);
    const result = try pager.get_page(2);
    std.debug.print("{s}", .{result});
}
