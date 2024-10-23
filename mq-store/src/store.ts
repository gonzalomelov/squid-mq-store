import {RedisClientType} from 'redis'

export class Store {
    private buffer: Array<[string, ...any[]]> = []
    private transaction: ReturnType<RedisClientType['multi']>

    constructor(transactionGetter: () => ReturnType<RedisClientType['multi']>) {
        this.transaction = transactionGetter()
    }

    hSet(key: string, field: string | object, value?: any) {
        if (typeof field === 'object') {
            this.buffer.push(['hSet', key, field])
        } else {
            this.buffer.push(['hSet', key, field, value])
        }
    }

    set(key: string, value: string) {
        this.buffer.push(['set', key, value])
    }

    get(key: string) {
        return this.transaction.get(key)
    }

    del(key: string) {
        this.buffer.push(['del', key])
    }

    rPush(key: string, ...values: string[]) {
        this.buffer.push(['rPush', key, ...values])
    }

    lRange(key: string, start: number, stop: number) {
        return this.transaction.lRange(key, start, stop)
    }

    exec(command: string, ...args: any[]) {
        this.buffer.push([command, ...args])
    }

    async flush() {
        for (const [command, ...args] of this.buffer) {
            await (this.transaction as any)[command](...args)
        }
        this.buffer = []
    }
}