# @gonzalomelov/mq-store

This package provides Redis-based database access for squid indexers.

## Usage example

```typescript
import {RedisDatabase} from '@gonzalomelov/mq-store'

const db = new RedisDatabase(options)

processor.run(db, async (ctx) => {
  for (let block of ctx.blocks) {
    for (let log of block.logs) {
      if (isTransferEvent(log)) {
        let {from, to, value} = decodeTransferEvent(log)
        
        ctx.store.hSet(`transfer:${log.id}`, {
          id: log.id,
          block: block.header.height,
          from,
          to,
          value: value.toString(),
          txnHash: log.transactionHash
        })
      }
    }
  }
})
```

This example demonstrates how to initialize a RedisDatabase and use it within a processor to store transfer events in Redis.