## Event Based Systems - Homework 1

- Team Composition:
  - Curca Andrei
  - Harabagiu Radu-Nicolae
  - Ochesanu Mihnea-Iorgu

### Homework description:
The program generates sets of publications and subscriptions based on the provided constraints as such:
    
- **PublicationSpout**: data source operator which represents the origin flux for the publications. The class generates the flux by creating a Publication object which is initialized with random data within constraints/taken from already established sets of data. Sample publication: **Publication(stationId=2, city=Vaslui, temp=18, rain=0.4642133198677695, wind=67, direction=NE, date=06.04.2023)**
- **SubscriptionSpout**: data source operator which represents the origin flux for the subscriptions. The flux is generated by creating a Subscription object  initialized with data according to given frequencies. Sample Subscription: **Subscription(entries=[SubscriptionEntry(field=date, operator==, value=02.04.2023), SubscriptionEntry(field=temperature, operator==, value=21.568056), SubscriptionEntry(field=stationid, operator==, value=2)])**
- **PubSubBolt**: data processing operator which receives the flux of data from the previously described spouts. In the cleanup function the bolt writes the publications and subscriptions inside an output text file.
- **PubSubTopology**: main class of the application which runs the topology, reuniting the operators and implementing a parallelization process using processes.

Parallelization:

- 10000 publications/10000 subscriptions/5 output files - 5 worker - 5 executors = 24.566S
- 10000 publications/10000 subscriptions/5 output files - 20 worker - 5 executors = 22.607S


The processes were run on an i5-13600KF processor: 14 cores/20 threads