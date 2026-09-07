// MongoDB 演示数据（docker-entrypoint-initdb.d 自动执行, root 权限无需认证）
// 集合: app_db.users（用户档案, 演示 MongoDB -> MySQL 同步）
db = db.getSiblingDB('app_db');

db.users.drop();

db.users.insertMany([
  { name: "Alice",   age: 28, email: "alice@example.com",   created_at: new Date("2026-08-01T10:00:00Z"), status: "active"   },
  { name: "Bob",     age: 34, email: "bob@example.com",     created_at: new Date("2026-08-02T11:30:00Z"), status: "active"   },
  { name: "Carol",   age: 22, email: "carol@example.com",   created_at: new Date("2026-08-03T09:15:00Z"), status: "inactive" },
  { name: "Dave",    age: 41, email: "dave@example.com",    created_at: new Date("2026-08-04T14:20:00Z"), status: "active"   },
  { name: "Eve",     age: 19, email: "eve@example.com",     created_at: new Date("2026-08-05T16:45:00Z"), status: "pending"  },
  { name: "Frank",   age: 37, email: "frank@example.com",   created_at: new Date("2026-08-06T08:05:00Z"), status: "active"   },
  { name: "Grace",   age: 26, email: "grace@example.com",   created_at: new Date("2026-08-07T13:40:00Z"), status: "inactive" },
  { name: "Henry",   age: 45, email: "henry@example.com",   created_at: new Date("2026-08-08T17:10:00Z"), status: "active"   }
]);

print("mongo-init: app_db.users inserted " + db.users.countDocuments() + " docs");
