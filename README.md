# How to run Locally
1. run `ant`
2. run the main ECS client using `java -jar m2-ecs.jar {port number}`
3. run the backup ECS client using `java -jar m2-ecs.jar {port number} {address of main ECS server (ex: localhost:50000)}`
4. run servers using `java -jar m2-server.jar {Server port} {Ecs port}`
5. run clients using `java -jar m2-client.jar`

Note: if the backup ecs fails another backup ecs server can be added using the same command, using the new port.