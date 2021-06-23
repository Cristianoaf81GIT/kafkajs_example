const conn  = require('./discover'); // import connection from discover file


const consumer = conn.consumer({groupId: 'test-group1'})

async function run() {
    await consumer.connect()
    await consumer.subscribe({topic: 'new-topic', fromBeginning: true})
    await consumer.run({
        eachMessage: async ({topic, partition, message}) => {
            console.log({
                message: message.value.toString(),
                topic: topic,
                partition: partition
            })
        }
    })
}

run().catch(console.log)