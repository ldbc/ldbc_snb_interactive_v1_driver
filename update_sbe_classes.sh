java -jar sbe-1.0.3-RC2.jar src/main/resources/metrics-schema.xml 
rm src/main/java/com/ldbc/driver/runtime/metrics/sbe/*
mv com/ldbc/driver/runtime/metrics/sbe/* src/main/java/com/ldbc/driver/runtime/metrics/sbe/
rm -rf com