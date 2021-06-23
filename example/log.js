const { Kafka, logLevel } = require('kafkajs')
const winston = require('winston')

const toLogLevel = level => {
    switch(level) {
        case logLevel.ERROR:
        case logLevel.NOTHING:
            return 'error'
        case logLevel.WARN:
            return 'warn'
        case logLevel.INFO:
            return 'info'
        case logLevel.DEBUG:
            return 'debug'
    }
}

const winstonlogCreator = logLevel => {
    const logger = winston.createLogger({
        level: toLogLevel(logLevel),
        transports: [
            new winston.transports.Console(),
            new winston.transports.File({filename: 'logger.log', level: 'info'})
        ]
    })
    return ({ namespace, level, label, log}) => {
        const { message, ...extra } = log
        logger.log({
            level: toLogLevel(level),
            message,
            extra
        })        
    }
}

const kafkaClient = new Kafka({
    clientId: 'logger',
    logLevel: logLevel.INFO,
    logCreator: winstonlogCreator,
    brokers: ['localhost:9092']
})


async function run() {
    kafkaClient.logger().info('connecting to apache kafka!')
    const consumer = kafkaClient.consumer({groupId: 'logger'})
    await consumer.connect()
    consumer.logger().info('this is a consumer')
    const producer = kafkaClient.producer()
    await producer.connect()
    producer.logger().info('this is a producer')
    producer.disconnect()
    consumer.disconnect()
    process.exit(1)
}

run().catch(console.log)
