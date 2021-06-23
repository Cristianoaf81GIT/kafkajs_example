const { Kafka } = require('kafkajs')

const client = new Kafka({
    clientId: 'admin-app',
    brokers: ['localhost:9092']
})

async function run(topicName) {
    const admin = client.admin({retry: 5})
    await admin.connect()
    const topics = await admin.listTopics()
    const groups = await admin.listGroups()
    console.log({
        topics,
        groups
    })
    // create a new topic
    await admin.createTopics({
        waitForLeaders: false,
        validateOnly: false,
        timeout: 1000,
        topics: [{
            topic: topicName,            
        }]
    })

    // example delete topics
    // await admin.deleteTopics({
    //     topics: <String[]>,
    //     timeout: <Number>,
    // })

    // uncomment below to delete new created topic
    // await admin.deleteTopics({
    //     topics: [topicName],
    //     timeout: 1000,
    // })
    await admin.disconnect()
}

run(process.argv[2].trim() || 'my-new-topic').catch(console.log)