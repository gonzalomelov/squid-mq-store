import {createClient, RedisClientType, RedisClientOptions} from 'redis'
import {HotDatabase, FinalTxInfo, HotTxInfo, HashAndHeight, HotDatabaseState} from '@subsquid/util-internal-processor-tools'
import {Store} from './store'

export interface RedisDatabaseOptions {
    url: string
    options?: RedisClientOptions
}

export class RedisDatabase implements HotDatabase<Store> {
    private client: RedisClientType
    public supportsHotBlocks = true as const

    constructor(private options: RedisDatabaseOptions) {
        this.client = createClient(options)
    }

    async connect(): Promise<HotDatabaseState> {
        await this.client.connect()
        const lastBlock = await this.client.get('lastProcessedBlock')
        const lastHash = await this.client.get('lastProcessedHash')
        const topBlocks = await this.client.lRange('topBlocks', 0, -1)
        
        const top: HashAndHeight[] = topBlocks.map(block => {
            const [height, hash] = block.split(':')
            return { height: parseInt(height), hash }
        })

        return {
            height: lastBlock ? parseInt(lastBlock) : -1,
            hash: lastHash || '0x',
            top
        }
    }

    async transact(info: FinalTxInfo, cb: (store: Store) => Promise<void>): Promise<void> {
        const transaction = this.client.multi()
        const store = new Store(() => transaction)
        await cb(store)
        await store.flush()
        transaction.set('lastProcessedBlock', info.nextHead.height.toString())
        transaction.set('lastProcessedHash', info.nextHead.hash)
        if (info.isOnTop) {
            transaction.rPush('topBlocks', `${info.nextHead.height}:${info.nextHead.hash}`)
        }
        await transaction.exec()
    }

    async transactHot(info: HotTxInfo, cb: (store: Store, block: HashAndHeight) => Promise<void>): Promise<void> {
        const transaction = this.client.multi()
        const store = new Store(() => transaction)
        
        for (const block of info.newBlocks) {
            await cb(store, block)
        }
        
        await store.flush()
        transaction.set('lastProcessedBlock', info.finalizedHead.height.toString())
        transaction.set('lastProcessedHash', info.finalizedHead.hash)
        
        transaction.del('topBlocks')
        for (const block of info.newBlocks) {
            transaction.rPush('topBlocks', `${block.height}:${block.hash}`)
        }
        
        await transaction.exec()
    }

    async transactHot2(
        info: HotTxInfo,
        cb: (store: Store, blockSliceStart: number, blockSliceEnd: number) => Promise<void>
    ): Promise<void> {
        const transaction = this.client.multi()
        const store = new Store(() => transaction)
        
        await cb(store, 0, info.newBlocks.length)
        
        await store.flush()
        transaction.set('lastProcessedBlock', info.finalizedHead.height.toString())
        transaction.set('lastProcessedHash', info.finalizedHead.hash)
        
        transaction.del('topBlocks')
        for (const block of info.newBlocks) {
            transaction.rPush('topBlocks', `${block.height}:${block.hash}`)
        }
        
        await transaction.exec()
    }

    async disconnect(): Promise<void> {
        await this.client.quit()
    }
}