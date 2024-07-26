const std = @import("std");
const Allocator = std.mem.Allocator;
const NODE_SIZE = 8;
const print = std.debug.print;

const BTreeNodeType = enum {
    Node,
    Leaf,
};

fn Next(comptime K: type, comptime V: type) type {
    return union(BTreeNodeType) {
        const Self = @This();

        Node: *BTreeNode(K, V),
        Leaf: *BTreeLeaf(K, V),

        fn insert(self: *Self, key: K, val: V) void {
            switch(self.*) {
                @This().Node => |node| node.insert(key, val),
                @This().Leaf => |leaf| leaf.insert(key, val),
            }
        }

        fn debug_print(self: Self) void {
            switch(self) {
                Self.Node => |node| node.debug_print(),
                Self.Leaf => |leaf| leaf.debug_print(),
            }
        }

        /// requires keys to be non empty
        fn first_key(self: Self) K {
            return switch(self) {
                Self.Node => |node| node.keys[0],
                Self.Leaf => |leaf| leaf.keys[0],
            };
        }
    };
}
fn KeyChild(comptime K: type, comptime V: type) type {
    const NextType = Next(K, V);
    return struct {
        key: K,
        child: NextType,
    };
}

fn BTreeLeaf(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();
        const KeyChildType = KeyChild(K, V);

        len: usize,
        keys: []K,
        // TODO this should maybe be a pointer?
        vals: []V,
        allocator: Allocator,

        const NextType = Next(K, V);

        fn to_next(self: *Self) NextType {
            return NextType { .Leaf = self };
        }

        /// returns the least upper bound from the current keyset of key
        fn lub(self: Self, key: K) usize {
            var i: usize = 0;
            while(i < self.len) : (i += 1) {
                if(self.keys[i] >= key) return i;
            }

            return self.len;
        }

        fn split(self: *Self) void {
            _ = self;
        }

        fn insert(self: *Self, key: K, val: V) !?*Self {
            if(self.len == self.keys.len) {
                const right = try BTreeLeaf(K, V).init(self.allocator);
                const split_point: usize = self.len / 2;
                // rhs has length ceil - 1, left has floor - 1
                const odd_split_difference: usize = if (self.len % 2 == 1) 1 else 0;
                right.len = split_point + odd_split_difference;

                @memcpy(right.keys[0..right.len], self.keys[split_point..self.keys.len]);
                @memcpy(right.vals[0..right.len], self.vals[split_point..self.vals.len]);

                self.len = split_point;

                return right;
            }

            const insert_loc = self.lub(key);

            if(self.keys[insert_loc] == key) {
                self.vals[insert_loc] = val;
                return null;
            }

            slice_insert(K, self.keys, insert_loc, key);
            slice_insert(V, self.vals, insert_loc, val);
            self.len += 1;
            return null;
        }

        fn init(allocator: Allocator) !*Self {
            const result = try allocator.create(BTreeLeaf(K, V));
            result.keys = try allocator.alloc(K, NODE_SIZE);
            result.vals = try allocator.alloc(V, NODE_SIZE);
            result.len = 0;
            result.allocator = allocator;
            return result;
        }

        fn debug_print(self: Self) void {
            for(self.keys, self.vals, 0..) |key, val, i| {
                if(i >= self.len) break;
                std.debug.print("({}: {}) ", .{key, val});
            }

            std.debug.print("\n", .{});
        }
    };
}


/// shifts everything to the right, drops end of slice.
fn slice_insert(comptime T: type, s: []T, pos: usize, val: T) void {
    // memcpy requires non overlapping :(
    for(pos+1..s.len) |i| {
        s[i] = s[i - 1];
    }
    s[pos] = val;
}

fn BTreeNode(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();
        const NextType = Next(K, V);
        const KeyChildType = KeyChild(K, V);

        len: usize,
        keys: []K,
        nexts: []NextType,
        allocator: Allocator,

        fn to_next(self: *Self) NextType {
            return NextType { .Node = self };
        }
        /// returns the least upper bound from the current keyset of key
        fn lub(self: Self, key: K) usize {
            var i: usize = 0;
            while(i < self.len) : (i += 1) {
                if(self.keys[i] >= key) return i;
            }

            return self.len;
        }

        fn add_key_child(self: *Self, child: NextType) !?*Self {
            if(self.len == self.keys.len) {
                const right = try BTreeNode(K, V).init(self.allocator);
                const split_point: usize = self.len / 2;
                const odd_split_difference: usize = if (self.len % 2 == 1) 1 else 0;
                right.len = split_point + odd_split_difference;
                @memcpy(right.keys[0..right.len], self.keys[split_point..self.keys.len]);
                @memcpy(right.nexts[1..right.len+1], self.nexts[split_point + 1..self.nexts.len]);
                right.nexts[0] = (try BTreeLeaf(K, V).init(self.allocator)).to_next();
                self.len = split_point;
                const key_to_insert = child.first_key();
                if(key_to_insert >= right.keys[0]) {
                    const key_child = try right.add_key_child(child);
                    std.debug.assert(key_child == null);
                } else {
                    const key_child = try self.add_key_child(child);
                    std.debug.assert(key_child == null);
                }
                return right;
            }

            const key_to_insert = child.first_key();
            const insert_loc = self.lub(key_to_insert);
            slice_insert(K, self.keys, insert_loc, key_to_insert);
            slice_insert(NextType, self.nexts, insert_loc + 1, child);
            self.len += 1;
            return null;
        }

        fn insert(self: *Self, key: K, val: V) !?*Self {
            if(self.len == 0) {
                self.keys[0] = key;
                self.nexts[0] = (try BTreeLeaf(K, V).init(self.allocator)).to_next();
                self.nexts[1] = (try BTreeLeaf(K, V).init(self.allocator)).to_next();
                _ = try self.nexts[1].Leaf.insert(key, val);
                self.len += 1;
                return null;
            }

            const loc = self.lub(key);
            const cur = self.nexts[loc];
            return switch(cur) {
                NextType.Node => |node| blkone: {
                    if(try node.insert(key, val)) |child| {
                        break :blkone self.add_key_child(child.to_next());
                    } else {
                        break :blkone null;
                    }
                },
                NextType.Leaf => |leaf| blktwo: {
                    if(try leaf.insert(key, val)) |child| {
                        break :blktwo self.add_key_child(child.to_next());
                    } else {
                        break :blktwo null;
                    }
                },
            };
        }

        fn init(allocator: Allocator) !*Self {
            const result = try allocator.create(BTreeNode(K, V));
            result.allocator = allocator;
            result.keys = try allocator.alloc(K, NODE_SIZE);
            result.nexts = try allocator.alloc(NextType, NODE_SIZE + 1);
            result.len = 0;
            return result;
        }

        fn debug_print(self: Self) void {
            for(self.nexts, 0..) |child, i| {
                // plus one because node always has one more child than key
                if(i >= self.len + 1) break;
                child.debug_print();
            }
        }
    };
}
fn BTree(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();

        // TODO having to type NextType in every struct feels weird
        const NextType = Next(K, V);

        // TODO maybe this should be a NextType rather than always a BTreeNode
        // only affects things when the tree is small so maybe not important
        root: ?*BTreeNode(K, V),
        allocator: Allocator,


        fn insert(self: *Self, key: K, val: V) !void {
            if(self.root == null) {
                self.root = try BTreeNode(K, V).init(self.allocator);
            }

            const right_child = try self.root.?.insert(key, val);
            if(right_child == null) return;

            const right_first_key = right_child.?.keys[0];

            // TODO other allocators?
            const new_root = try BTreeNode(K, V).init(self.allocator);
            const old_root = self.root.?;

            new_root.keys[0] = right_first_key;
            new_root.nexts[0] = old_root.to_next();
            new_root.nexts[1] = right_child.?.to_next();
            new_root.len = 1;

            self.root = new_root;
        }


        fn init(allocator: Allocator) Self {
            return BTree(K, V){ .root = null, .allocator = allocator };
        }

        fn debug_print(self: Self) void {
            if(self.root == null) {
                return;
            }

            self.root.?.debug_print();
        }

        fn deinit(self: *Self) void {
            self.root.deinit();
            self.allocator.destroy(self.root);
        }
    };
}

test "btree works" {
    var tree = BTree(i32, i32).init(std.testing.allocator);
    try tree.insert(2, 2);
    try tree.insert(3, 3);
    try tree.insert(0, 0);
    try tree.insert(1, 1);
    try tree.insert(4, 4);
    try tree.insert(5, 5);

    tree.debug_print();
}

test "splitting works" {
    var tree = BTree(usize, usize).init(std.heap.page_allocator);
    for(0..128) |i| {
        try tree.insert(i, i);
    }

    tree.debug_print();
    try std.testing.expect(false);
}
