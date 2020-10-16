# CDAP - Static Mapping transform plugin

A CDAP plugin that provides a transform for mapping values from a lookup table provided as a
JSON document.

## build
To build the plugin run

```
mvn clean package -DskipTests
```

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy the plugin.


## Deployment
You can deploy your plugins using the CDAP CLI:

    > load artifact target/static-mapper-0.1.0.jar config-file target/static-mapper-0.1.0.json
