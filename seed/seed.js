#!/usr/bin/env node
// seed determinístico — seed=42 — tabela: customers 200 | accounts 240 | cards 320 | merchants 60 | transactions 2000
let s = 42;
const rnd = () => (s = (s * 1103515245 + 12345) % 2147483648) / 2147483648;
const int = (lo, hi) => lo + Math.floor(rnd() * (hi - lo + 1));

console.log("-- seed=42 — seed determinístico (LCG mod 2^31)");
console.log("-- customers=200 accounts=240 cards=320 merchants=60 transactions=2000");

const customers = Array.from({ length: 200 }, (_, i) => ({
  customer_id: i + 1,
  customer_name: `Customer ${i + 1}`,
  customer_email: `customer${i + 1}@desafio.com`,
  customer_segment: ["retail", "food", "fuel", "health"][i % 4],
}));
console.log("INSERT INTO customers (customer_id, customer_name, customer_email, customer_segment) VALUES");
customers.forEach((c, i) => {
  console.log(`  (${c.customer_id}, '${c.customer_name}', '${c.customer_email}', '${c.customer_segment}')${i < customers.length - 1 ? "," : ""};`);
});
console.log(";");

const accounts = Array.from({ length: 240 }, (_, i) => ({
  account_id: i + 1,
  customer_id: (i % 200) + 1,
  balance_cents: int(1000, 900000),
  status: i % 7 === 0 ? "frozen" : "active",
}));
console.log("INSERT INTO accounts (account_id, customer_id, balance_cents, status) VALUES");
accounts.forEach((a, i) => {
  console.log(`  (${a.account_id}, ${a.customer_id}, ${a.balance_cents}, '${a.status}')${i < accounts.length - 1 ? "," : ""};`);
});
console.log(";");

const cards = Array.from({ length: 320 }, (_, i) => ({
  card_id: i + 1,
  account_id: int(1, 240),
  card_number: `****${String(int(1000, 9999))}`,
  status: "active",
}));
cards.push({ card_id: 1001, account_id: 11, card_number: "****1001", status: "active" });
console.log("INSERT INTO cards (card_id, account_id, card_number, status) VALUES");
cards.forEach((c, i) => {
  console.log(`  (${c.card_id}, ${c.account_id}, '${c.card_number}', '${c.status}')${i < cards.length - 1 ? "," : ""};`);
});
console.log(";");

const merchants = Array.from({ length: 60 }, (_, i) => ({
  merchant_id: i + 1,
  mcc: String(1000 + int(0, 600)).slice(0, 4),
  merchant_name: `Merchant ${i + 1}`,
}));
console.log("INSERT INTO merchants (merchant_id, mcc, merchant_name) VALUES");
merchants.forEach((m, i) => {
  console.log(`  (${m.merchant_id}, '${m.mcc}', '${m.merchant_name}')${i < merchants.length - 1 ? "," : ""};`);
});
console.log(";");

const txs = Array.from({ length: 2000 }, (_, i) => ({
  transaction_id: i + 1,
  account_id: int(1, 240),
  card_id: int(1, 320),
  merchant_id: int(1, 60),
  amount_cents: int(100, 90000),
  event_ts: int(1, 86400000) * 1000,
}));
txs[100] = { transaction_id: 101, account_id: 11, card_id: 1001, merchant_id: 5, amount_cents: 12000, event_ts: 1000000 };
txs[401] = { transaction_id: 402, account_id: 11, card_id: 1001, merchant_id: 17, amount_cents: 24000, event_ts: 3000000 };
txs[802] = { transaction_id: 803, account_id: 11, card_id: 1001, merchant_id: 33, amount_cents: 15000, event_ts: 5900000 };
console.log("INSERT INTO transactions (transaction_id, account_id, card_id, merchant_id, amount_cents, event_ts) VALUES");
txs.forEach((t, i) => {
  console.log(`  (${t.transaction_id}, ${t.account_id}, ${t.card_id}, ${t.merchant_id}, ${t.amount_cents}, ${t.event_ts})${i < txs.length - 1 ? "," : ""};`);
});
console.log(";");
