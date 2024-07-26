const std = @import("std");
const print = std.debug.print;
const Allocator = std.mem.Allocator;

pub fn TreeNode(comptime K: type, comptime V: type) type {
    return struct {
        key: K,
        val: V,
        left: ?*@This(),
        right: ?*@This(),

        fn init(key: K, val: V, allocator: Allocator) !*@This() {
            const result = try allocator.create(TreeNode(K, V));
            result.key = key;
            result.val = val;
            result.left = null;
            result.right = null;
            return result;
        }

        fn deinit(self: @This(), allocator: Allocator) void {
            if(self.left != null) {
                self.left.?.deinit(allocator);
                allocator.destroy(self.left.?);
            }

            if(self.right != null) {
                self.right.?.deinit(allocator);
                allocator.destroy(self.right.?);
            }
        }

        fn debug_print(self: @This()) void {
            if(self.left != null) self.left.?.debug_print();
            print("({any} : {any}) ", .{self.key, self.val});
            if(self.right != null) self.right.?.debug_print();
        }
    };
}

pub fn Tree(comptime K: type, comptime V: type) type {
    return struct {
        root: ?*TreeNode(K, V),
        allocator: Allocator,

        fn insert(self: *@This(), key: K, val: V) !void {
            if(self.root == null) {
                self.root = try TreeNode(K, V).init(key, val, self.allocator);
                return;
            }

            var cur = self.root.?;

            while(true) {
                if(cur.key == key) {
                    cur.val = val;
                    return;
                }

                if(cur.key < key) {
                    if(cur.right == null) {
                        cur.right = try TreeNode(K, V).init(key, val, self.allocator);
                        return;
                    }
                    cur = cur.right.?;
                    continue;
                }

                if(cur.left == null) {
                    cur.left = try TreeNode(K, V).init(key, val, self.allocator);
                    return;
                }

                cur = cur.left.?;
            }

            unreachable;
        }

        fn get(self: @This(), key: K) ?V {
            var cur = self.root;
            while(cur != null) {
                if(cur.?.key == key) {
                    return cur.?.val;
                }

                if(cur.?.key < key) {
                    cur = cur.?.right;
                    continue;
                }

                cur = cur.?.left;
            }

            return null;
        }

        fn init(allocator: Allocator) @This() {
            return Tree(K, V) {
                .allocator = allocator,
                .root = null,
            };
        }

        fn deinit(self: *@This()) void {
            if(self.root == null) return;
            self.root.?.deinit(self.allocator);
            self.allocator.destroy(self.root.?);
        }

        fn debug_print(self: @This()) void {
            if(self.root == null) {
                return;
            }
            self.root.?.debug_print();
            print("\n", .{});
        }
    };

}
test "tree_works" {
    const tree = Tree(i32, i32).init(std.testing.allocator);
    _ = tree;
    print("tree test ok!\n", .{});
}

test "insert_works" {
    var tree = Tree(i32, i32).init(std.testing.allocator);
    try tree.insert(1, 10);
    try tree.insert(2, 20);
    try tree.insert(-1, 30);
    try tree.insert(1, 40);

    tree.debug_print();
    const result_one = tree.get(1);
    const result_two = tree.get(2);
    const result_minus_one = tree.get(-1);

    try std.testing.expect(result_one != null);
    try std.testing.expect(result_two != null);
    try std.testing.expect(result_minus_one != null);
    try std.testing.expect(result_one == 40);
    try std.testing.expect(result_two == 20);
    try std.testing.expect(result_minus_one == 30);

    tree.deinit();

    print("tree insert and get test ok!\n", .{});
}
