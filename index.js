require('dotenv').config();
const { Kafka, Partitioners } = require('kafkajs');

const inputTopic = process.env.KAFKA_INPUT_TOPIC;
const outputTopic = process.env.KAFKA_OUTPUT_TOPIC;
const kafkaBroker = process.env.KAFKA_BROKER; // Get broker from env variable

const kafka = new Kafka({
    clientId: 'json-consumer',
    brokers: [kafkaBroker],
    sasl: {
        mechanism: "scram-sha-512",
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD,
    },
});

// Dynamic groupId (new group on each run)
const consumer = kafka.consumer({ groupId: `data-processing-group-${Date.now()}` });

const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner // Fix KafkaJS partitioner warning
});

const consumeMessages = async () => {
    try {
        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: inputTopic, fromBeginning: true });

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                try {
                    const value = message.value.toString();
                    console.log(`📥 Consumed Message:`, value);

                    // Directly publishing the consumed message without any condition
                    await producer.send({
                        topic: outputTopic,
                        messages: [{ value }]
                    });

                    console.log(`🚀 Published Message to ${outputTopic}`);
                } catch (error) {
                    if (error.message.includes("The group is rebalancing")) {
                        console.warn("⚠️ Consumer is rebalancing, reconnecting...");
                        await consumer.disconnect();
                        await consumer.connect();
                        await consumer.subscribe({ topic: inputTopic, fromBeginning: true });
                    } else {
                        console.error('❌ Error processing message:', error);
                    }
                }
            }
        });
    } catch (error) {
        console.error("🚨 Error in Kafka consumer setup:", error);
    }
};

consumeMessages().catch(console.error);