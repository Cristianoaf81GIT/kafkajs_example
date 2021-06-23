const { Kafka } = require('kafkajs');


const conn = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
})

const consumer = conn.consumer({ groupId: 'test-group' })

async function run() {
    await consumer.connect();
    await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                value: message.value.toString()
            })
        }
    })
}

run().catch(console.error)