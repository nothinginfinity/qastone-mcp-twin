# QA.Stone MCP Twin Server

**Zero-downtime MCP server for QA.Stone demo system**

This project demonstrates MCP (Model Context Protocol) hot-swap capability using a twin-server architecture (A/B deployment). Designed for Railway deployment.

## Architecture

```
                    ┌─────────────────────────────────────────┐
                    │            Railway Cloud                │
                    │                                         │
  Public URLs       │  ┌─────────────┐   ┌─────────────┐     │
  qastone-a.up...   │  │   MCP-A     │   │   MCP-B     │     │
  qastone-b.up...   │  │   :8301     │   │   :8302     │     │
                    │  └──────┬──────┘   └──────┬──────┘     │
                    │         │                  │            │
                    │         └────────┬─────────┘            │
                    │                  ▼                      │
                    │          ┌─────────────┐                │
                    │          │    Redis    │                │
                    │          └─────────────┘                │
                    └─────────────────────────────────────────┘
```

## Features

- **MCP Protocol**: Full JSON-RPC MCP implementation
- **REST API**: Direct HTTP endpoints for easy testing
- **Redis-backed**: Persistent accounts, wallets, and transfers
- **Hot-swap ready**: Twin servers for zero-downtime updates
- **Demo accounts**: 5 pre-seeded accounts with QA.Stones

## Quick Start

### 1. Set Environment Variables

```bash
export REDIS_HOST=your-redis-host.com
export REDIS_PORT=11026
export REDIS_USER=default
export REDIS_PASSWORD=your-password
export SERVER_INSTANCE=a
export SERVER_PORT=8301
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Seed Demo Accounts

```bash
python seed_accounts.py --reset
```

### 4. Run Server

```bash
python qastone_server.py
```

## Demo Accounts

| Username | Token | Starting Value |
|----------|-------|----------------|
| alice | `demo_alice_2026` | $40.00 |
| bob | `demo_bob_2026` | $50.00 |
| charlie | `demo_charlie_2026` | $50.00 |
| diana | `demo_diana_2026` | $50.00 |
| eve | `demo_eve_2026` | $50.00 |

## API Endpoints

### MCP Protocol

```bash
# Initialize
curl -X POST http://localhost:8301/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize"}'

# List tools
curl -X POST http://localhost:8301/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'

# Call tool
curl -X POST http://localhost:8301/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"qastone_list_users","arguments":{}}}'
```

### REST API (easier testing)

```bash
# List users
curl http://localhost:8301/api/users

# Get wallet
curl http://localhost:8301/api/users/alice/wallet

# Send stone
curl -X POST "http://localhost:8301/api/transfer?from_token=demo_alice_2026&to_username=bob&stone_id=GI_amazon_xxx"

# Check inbox
curl http://localhost:8301/api/inbox/demo_bob_2026

# Accept transfer
curl -X POST "http://localhost:8301/api/accept?token=demo_bob_2026&transfer_id=tx_xxx"

# System stats
curl http://localhost:8301/api/stats
```

## MCP Tools

| Tool | Description |
|------|-------------|
| `qastone_create_account` | Create user account |
| `qastone_list_users` | List all users |
| `qastone_user_wallet` | Get user's wallet |
| `qastone_send_to_user` | Send stone between users |
| `qastone_check_inbox` | Check pending transfers |
| `qastone_accept_transfer` | Accept incoming stone |
| `qastone_mint_to_user` | Mint new stone to user |
| `qastone_system_stats` | System statistics |

## Railway Deployment

### Deploy Twin Services

1. Create new Railway project
2. Link this GitHub repo
3. Create two services from the same repo:
   - **qastone-a**: Set `SERVER_INSTANCE=a`, `SERVER_PORT=8301`
   - **qastone-b**: Set `SERVER_INSTANCE=b`, `SERVER_PORT=8302`
4. Add Redis service (or use existing)
5. Set Redis environment variables for both services

### Environment Variables (Railway)

```
REDIS_HOST=<your-redis-host>
REDIS_PORT=<port>
REDIS_USER=default
REDIS_PASSWORD=<password>
SERVER_INSTANCE=a  # or 'b' for second service
SERVER_PORT=8301   # or 8302 for second service
SERVER_VERSION=1.0.0
```

## Hot-Swap Demo

1. Both servers (A and B) run simultaneously
2. Update code on server B while A handles traffic
3. Deploy B, verify it's healthy
4. Switch traffic from A to B (via DNS or load balancer)
5. Update A while B handles traffic
6. Zero downtime achieved!

## Testing Transfer Flow

```bash
# 1. Check Alice's wallet
curl http://localhost:8301/api/users/alice/wallet

# 2. Alice sends stone to Bob
curl -X POST "http://localhost:8301/api/transfer?from_token=demo_alice_2026&to_username=bob&stone_id=<stone_id>"

# 3. Check Bob's inbox
curl http://localhost:8301/api/inbox/demo_bob_2026

# 4. Bob accepts the transfer
curl -X POST "http://localhost:8301/api/accept?token=demo_bob_2026&transfer_id=<transfer_id>"

# 5. Verify Bob has the stone
curl http://localhost:8301/api/users/bob/wallet
```

## License

MIT
