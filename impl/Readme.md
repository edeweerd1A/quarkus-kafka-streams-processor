# __DEPRECATED__ Implementation

This is the main module of the library that provides most of the features of the extension.
It is expected to be put on the classpath of the streaming application.
However, the application should not need it as a compile dependency. 

## Deprecation

This module was discontinued and emptied in favour of using directly the runtime module of the extension because otherwise the extension configuration was never detected as a runtime configuration but a buildtime configuration.
One of the notifiable consequences was the incapacity to change the `kafkastreamsprocessor.error-strategy` at runtime.
I.e. this setting is rather an operation one, so makes no sense to define it at build time.

Only fix found was to abandon this module and move the rest of the code to runtime, so that the `@ConfigMapping(prefix = "kafkastreamsprocessor")` annotation could be removed from `quarkus-kafka-streams-processor-spi`.
