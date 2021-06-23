const { Kafka } = require('kafkajs');
const fetch = require('node-fetch');

const conn = new Kafka({
    clientId: 'my-app-client-discover',
    brokers: async () => {
        // get cluster from rest proxy on localhost only
        // see on https://kafka.js.org/docs/configuration
        const clusterResponse = await fetch('http://localhost:8082/v3/clusters', {
            method: 'get',
            headers: {accept: 'application/json'},
        }).then(response => response.json())
        
        // get cluster url (must replace rest-proxy for localhost)
        const clusterUrl = clusterResponse.data[0].brokers.related.replace('rest-proxy', 'localhost')
        
        // get brokers from clusters 
        const brokerResponse = await fetch(`${clusterUrl}`,{
            method: 'get',
            headers: {accept:'application/json'},
        }).then(response => response.json())
        
        // get host and port for connection from brokers
        const brokers = brokerResponse.data.map(broker => {
            let { host, port } = broker
            // port with 5 chars length then remove first char
            port && String(port).length === 5 && (port = Number(String(port).substring(1)))
            // replace broker name to localhost
            return `${host.replace('broker','localhost')}:${port}`
        })
        // return brockers
        return brokers
    }
})

async function run() {
    const producer = conn.producer({allowAutoTopicCreation: true})
    await producer.connect()
    await producer.send({
        topic: 'new-topic',
        messages: [
            { value: 'A message from discovered broker' }
        ]
    })
    await producer.disconnect()    
}

run().catch(console.error)

module.exports=conn

// example
/**
 * const kafka = new Kafka({
  clientId: 'my-app',
  brokers: async () => {
    // Example getting brokers from Confluent REST Proxy
    const clusterResponse = await fetch('https://kafka-rest:8082/v3/clusters', {
      headers: 'application/vnd.api+json',
    }).then(response => response.json())
    const clusterUrl = clusterResponse.data[0].links.self

    const brokersResponse = await fetch(`${clusterUrl}/brokers`, {
      headers: 'application/vnd.api+json',
    }).then(response => response.json())

    const brokers = brokersResponse.data.map(broker => {
      const { host, port } = broker.attributes
      return `${host}:${port}`
    })

    return brokers
  }
})
 */
