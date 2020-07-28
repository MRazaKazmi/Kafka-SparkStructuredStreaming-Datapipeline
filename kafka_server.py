import producer_server


def run_kafka_server():
    input_file = "./police-department-calls-for-service.json"
    
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="udacity.police.calls",
        bootstrap_servers=["localhost:9092"],
        client_id=""
    )

    return producer


def feed():
    print("Starting Kafka Producer")
    producer = run_kafka_server()
    print("Starting Data Generation")
    producer.generate_data()


if __name__ == "__main__":
    feed()
