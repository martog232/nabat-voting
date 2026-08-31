package com.example.nabatvoting.infrastructure.kafka;

import org.apache.avro.Schema;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;
import org.example.nabat.events.vote.VoteChanged;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * The compatibility gate, in the build rather than at deploy time.
 *
 * <p>A schema registry refuses an incompatible schema, which is what stops a bad version
 * reaching consumers — but only once something tries to register it, which is a service
 * starting in an environment that has a registry. That is late: the change is written,
 * reviewed, merged and built by then. This test asks the same question of the same schemas,
 * with no registry and no broker, at the point where the answer is cheap to act on.
 *
 * <p><b>Transitive, not just against the previous version.</b> The registry's BACKWARD level
 * compares a new schema with the latest one only. That is not enough here: nabat-app consumes
 * with {@code auto-offset-reset=earliest} and rebuilds its projection by replaying the topic
 * from the beginning, so it can meet a message written by <em>any</em> version that ever
 * published — not just the one before this. Every historical schema is checked.
 *
 * <h2>Changing the schema</h2>
 * Edit {@code src/main/avro/VoteChanged.avsc}; this test tells you whether consumers can still
 * read what is already on the topic. When a version is published, copy the file to
 * {@code src/test/resources/schema-history/VoteChanged.vN.avsc} — the history is what makes
 * the check possible, and a version missing from it is a version nothing protects.
 */
class VoteChangedSchemaCompatibilityTest {

    private static final String HISTORY_DIRECTORY = "/schema-history";

    /**
     * @return the published versions, oldest first. Ordered by the number in the file name
     *     rather than lexically, so v10 does not sort before v2.
     */
    private static List<Schema> publishedVersions() {
        try {
            Path directory = Path.of(Objects.requireNonNull(
                    VoteChangedSchemaCompatibilityTest.class.getResource(HISTORY_DIRECTORY),
                    "no " + HISTORY_DIRECTORY + " on the test classpath").toURI());

            try (Stream<Path> files = Files.list(directory)) {
                return files
                        .sorted(Comparator.comparingInt(VoteChangedSchemaCompatibilityTest::versionOf))
                        .map(VoteChangedSchemaCompatibilityTest::parse)
                        .toList();
            }
        } catch (IOException | URISyntaxException e) {
            throw new UncheckedIOException("Could not read the schema history",
                    e instanceof IOException io ? io : new IOException(e));
        }
    }

    private static int versionOf(Path file) {
        String name = file.getFileName().toString();
        return Integer.parseInt(name.replaceAll("^.*\\.v(\\d+)\\.avsc$", "$1"));
    }

    private static Schema parse(Path file) {
        try (InputStream in = Files.newInputStream(file)) {
            return new Schema.Parser().parse(in);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not parse " + file, e);
        }
    }

    @Test
    void theCurrentSchemaCanReadEveryVersionEverPublished() {
        List<Schema> published = publishedVersions();
        assertThat(published)
                .as("the history is the only record of what is already on the topic")
                .isNotEmpty();

        SchemaValidator canReadAllOfThem = new SchemaValidatorBuilder()
                .canReadStrategy()
                .validateAll();

        assertThatCode(() -> canReadAllOfThem.validate(VoteChanged.getClassSchema(), published))
                .as("""
                    src/main/avro/VoteChanged.avsc can no longer read data written by a version \
                    already published. Consumers replay this topic from the beginning, so those \
                    messages will be read again. Add a default to the new field, or keep the old \
                    one and deprecate it — do not rename.""")
                .doesNotThrowAnyException();
    }

    /**
     * The record is still called what it was called.
     *
     * <p>The one break a compatibility check cannot see. Renaming the record or its namespace
     * produces a schema that is perfectly valid and describes a different type: the registry
     * compares fields, so a rename reads to it as a deletion and an addition and can pass.
     * On the consumer's side it is fatal in a way nothing warns about — nabat-app generates
     * its class from a copy of this file, and the specific reader resolves by full name.
     */
    @Test
    void theRecordIsStillCalledWhatItWasCalled() {
        String currentName = VoteChanged.getClassSchema().getFullName();

        assertThat(publishedVersions())
                .extracting(Schema::getFullName)
                .as("a renamed record is a different type to every consumer that has the old one")
                .containsOnly(currentName);
    }
}
