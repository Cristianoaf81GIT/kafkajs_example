const { Kafka } = require('kafkajs');


const conn = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
})

async function run() {
    // allow the producer to auto create a topic
    const producer = conn.producer({allowAutoTopicCreation: true})

    await producer.connect()
    await producer.send({
        topic: 'test-topic',
        messages: [
            { value: JSON.stringify({
                name: 'jhon doe',
                age: 32,
                gender: 'M',
                email: 'jhondoe@email.com',
                password: 'svp3rs3c4t'
            })}
        ]
    })

    await producer.disconnect()
}

run().catch(console.error)