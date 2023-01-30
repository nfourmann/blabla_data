# BlaBla Data!

Here the codebase to queries, process and store data coming from Netherlands Public Transport.

About the project structure, 
- we can find all API code into infrasctruture where there are connectors and clients. 
- Business objects are under the domain folder and propose an business understanding and view of data. 
- The ETL workflows are encoded into the dag folder where you can find all the upload/copy logic. 
- As for the contracts folder, it proposes a way to easily identify data producers and consumers and the link between.
