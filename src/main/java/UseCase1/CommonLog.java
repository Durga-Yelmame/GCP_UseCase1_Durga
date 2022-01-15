package UseCase1;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;


@DefaultSchema(JavaFieldSchema.class)
public class CommonLog {
    @javax.annotation.Nullable int id;
    @javax.annotation.Nullable String name;
    @javax.annotation.Nullable String surname;
}
