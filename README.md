# Hand's on Apache Flink:

This code was used to write an Apache Flink job and export it as a jar and import it into an existing data mesh.

Data is read in from an Apache Kafka and then evaluated using a serializer, hashed and then written to an existing MongoDB database to see the evaluation there.

More information about the setup and the commands can be found here:
https://gitlab.com/RaLazo/customerdataproducer
