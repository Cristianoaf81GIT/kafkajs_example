const { Kafka } = require('kafkajs')

const client = new Kafka({
    clientId: 'transactional-client',
    brokers: ['localhost:9092']
})
// https://kafka.js.org/docs/transactions
const producer = client.producer({maxInFlightRequests: 1, idempotent: true, transactionalId: 'trx'})

async function run() {
    try {
        await producer.connect() // the producer must be connected
        const transaction = await producer.transaction()
        await transaction.send({
            topic: 'new-topic',
            messages: [
                { value: JSON.stringify({
                    title: 'get start with apache kafka and node js',
                    value: 'R$ 13,00',
                    author: 'unknown'
                })}
            ]
        })
        await transaction.commit()
        console.log('message sent')
        process.exit(1)
    } catch (error) {
        console.log(error)
        await transaction.abort()
    } finally {
        await producer.disconnect()
    }
}

run().catch(console.error)