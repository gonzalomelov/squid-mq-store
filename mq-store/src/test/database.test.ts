import expect from 'expect'
import { RedisDatabase, RedisDatabaseOptions } from '../database'
import { createClient, RedisClientType } from 'redis'

describe('RedisDatabase', function() {
    let redisClient: RedisClientType
    let db: RedisDatabase
    let options: RedisDatabaseOptions = {
        url: 'redis://localhost:6379'
    }

    beforeEach(async () => {
        redisClient = createClient(options)
        await redisClient.connect()
        await redisClient.flushAll()
        db = new RedisDatabase(options)
    })

    afterEach(async () => {
        await db.disconnect()
        await redisClient.quit()
    })

    it('initial connect', async function() {
        let state = await db.connect()
        expect(state).toMatchObject({height: -1, hash: '0x', top: []})
    })

    it('.transact() flow', async function() {
        await db.connect()

        await db.transact({
            prevHead: {height: -1, hash: '0x'},
            nextHead: {height: 10, hash: '0x10'},
            isOnTop: true
        }, async store => {
            store.hSet('transfer:1', {
                id: '1',
                from: 'address1',
                to: 'address2',
                value: '100'
            })
        })

        await db.transact({
            prevHead: {height: 10, hash: '0x10'},
            nextHead: {height: 20, hash: '0x20'},
            isOnTop: true
        }, async store => {
            store.hSet('transfer:2', {
                id: '2',
                from: 'address2',
                to: 'address3',
                value: '200'
            })
        })

        let transfer1 = await redisClient.hGetAll('transfer:1')
        let transfer2 = await redisClient.hGetAll('transfer:2')

        expect(transfer1).toMatchObject({
            id: '1',
            from: 'address1',
            to: 'address2',
            value: '100'
        })

        expect(transfer2).toMatchObject({
            id: '2',
            from: 'address2',
            to: 'address3',
            value: '200'
        })

        await db.disconnect()

        expect(await db.connect()).toMatchObject({
            height: 20,
            hash: '0x20',
            top: [
                {height: 10, hash: '0x10'},
                {height: 20, hash: '0x20'}
            ]
        })
    })

    it('.transactHot() flow', async function() {
        await db.connect()

        await db.transactHot({
            baseHead: {height: -1, hash: '0x'},
            newBlocks: [
                {height: 0, hash: '0'},
            ],
            finalizedHead: {height: 0, hash: '0'}
        }, async () => {})

        await db.transactHot({
            baseHead: {height: 0, hash: '0'},
            finalizedHead: {height: 0, hash: '0'},
            newBlocks: [
                {height: 1, hash: 'a-1'},
                {height: 2, hash: 'a-2'},
                {height: 3, hash: 'a-3'}
            ]
        }, async (store, block) => {
            switch(block.height) {
                case 1:
                    store.hSet('transfer:1', {
                        id: '1',
                        from: 'address1',
                        to: 'address2',
                        value: '100'
                    })
                    break
                case 2:
                    store.hSet('transfer:2', {
                        id: '2',
                        from: 'address2',
                        to: 'address3',
                        value: '200'
                    })
                    break
            }
        })

        let transfer1 = await redisClient.hGetAll('transfer:1')
        let transfer2 = await redisClient.hGetAll('transfer:2')

        expect(transfer1).toMatchObject({
            id: '1',
            from: 'address1',
            to: 'address2',
            value: '100'
        })

        expect(transfer2).toMatchObject({
            id: '2',
            from: 'address2',
            to: 'address3',
            value: '200'
        })

        await db.disconnect()

        expect(await db.connect()).toMatchObject({
            height: 0,
            hash: '0',
            top: [
                {height: 1, hash: 'a-1'},
                {height: 2, hash: 'a-2'},
                {height: 3, hash: 'a-3'}
            ]
        })
    })
})
