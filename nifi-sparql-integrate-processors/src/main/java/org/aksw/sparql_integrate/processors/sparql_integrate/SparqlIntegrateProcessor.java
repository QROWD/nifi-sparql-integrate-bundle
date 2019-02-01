/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.aksw.sparql_integrate.processors.sparql_integrate;

import com.google.common.collect.Streams;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;
import org.aksw.jena_sparql_api.sparql.ext.http.JenaExtensionHttp;
import org.aksw.jena_sparql_api.sparql.ext.util.JenaExtensionUtil;
import org.aksw.jena_sparql_api.stmt.SparqlStmt;
import org.aksw.jena_sparql_api.stmt.SparqlStmtIterator;
import org.aksw.jena_sparql_api.stmt.SparqlStmtParserImpl;
import org.aksw.jena_sparql_api.stmt.SparqlStmtQuery;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.out.SinkQuadOutput;
import org.apache.jena.riot.out.SinkTripleOutput;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.shared.impl.PrefixMappingImpl;
import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.lang.arq.ParseException;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateProcessor;
import org.apache.jena.update.UpdateRequest;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"RDF", "SPARQL"})
@CapabilityDescription("This processor takes an SPARQL query as an argument and outputs a RDF-Turtle file.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "mime.type", description = "Idnetify what kind of Input is in FlowFile")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
// @InputRequirement(Requirement.INPUT_REQUIRED)
public class SparqlIntegrateProcessor extends AbstractProcessor {

    public interface FLOW_FILE_CONTENTS {
        String RDF_DATA = "rdfData";
        String SPARQL_QUERY = "sparqlQuery";
        String NON_RDF_DATA = "nonRdfData";
        String EMPTY = "empty";
    }

    public static final AllowableValue RDF_DATA = new AllowableValue(FLOW_FILE_CONTENTS.RDF_DATA);
    public static final AllowableValue SPARQL_QUERY = new AllowableValue(FLOW_FILE_CONTENTS.SPARQL_QUERY);
    public static final AllowableValue NON_RDF_DATA = new AllowableValue(FLOW_FILE_CONTENTS.NON_RDF_DATA);
    public static final AllowableValue EMPTY = new AllowableValue(FLOW_FILE_CONTENTS.EMPTY);

    public static final PropertyDescriptor BASE_URI = new PropertyDescriptor.Builder()
            .name("BASE_URI")
            .displayName("Base URI")
            .description("Base URI for the SPARQL queries.")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor SPARQL_QUERY_PROPERTY = new PropertyDescriptor.Builder()
            .name("SPARQL_QUERY")
            .displayName("SPARQL Query")
            .description("The SPARQL query to run.")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONTENT_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("Content of FlowFile")
            .displayName("Content of the processors input FlowFile")
            .description("Content of the processors input FlowFile")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .allowableValues(RDF_DATA, SPARQL_QUERY, NON_RDF_DATA, EMPTY)
            .defaultValue(RDF_DATA.getValue())
            .build();

    public static final Relationship SUCCESS =
            new Relationship.Builder().name("SUCCESS").description("Success relationship").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(BASE_URI);
        descriptors.add(CONTENT_FLOW_FILE);
        descriptors.add(SPARQL_QUERY_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        final String contentFlowFile = context.getProperty(CONTENT_FLOW_FILE).getValue();
        final String baseUri = context.getProperty(BASE_URI).getValue();
        final AtomicReference<String> sparqlQuery =
                new AtomicReference<String>(context.getProperty(SPARQL_QUERY_PROPERTY).getValue());
        final AtomicReference<Stream<SparqlStmt>> stmts = new AtomicReference<>();

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        PrefixMapping pm = new PrefixMappingImpl();
        pm.setNsPrefixes(PrefixMapping.Extended);
        JenaExtensionUtil.addPrefixes(pm);
        JenaExtensionHttp.addPrefixes(pm);
        Dataset dataset = DatasetFactory.create();
        RDFConnection conn = RDFConnectionFactory.connect(dataset);
        Prologue prologue = new Prologue();
        prologue.setPrefixMapping(pm);
        prologue.setBaseURI(baseUri);
        Function<String, SparqlStmt> sparqlStmtParser = SparqlStmtParserImpl.create(Syntax.syntaxARQ, prologue, true);

        final Path path = Paths.get(baseUri, flowFile.getAttribute("filename"));
        switch (contentFlowFile) {
            case FLOW_FILE_CONTENTS.SPARQL_QUERY:
                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(InputStream in) throws IOException {
                        sparqlQuery.set(CharStreams.toString(new InputStreamReader(in, StandardCharsets.UTF_8)));
                    }
                });
                break;
            case FLOW_FILE_CONTENTS.RDF_DATA:
                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(InputStream in) throws IOException {
                        RDFDataMgr.read(dataset, in, baseUri, Lang.TURTLE);
                    }
                });
                break;
            case FLOW_FILE_CONTENTS.NON_RDF_DATA:
                session.exportTo(flowFile, path, false);
                break;
        }

        try {
            stmts.set(parseSparqlQuery(sparqlQuery.get(), sparqlStmtParser));
        } catch (Exception ex) {
            ex.printStackTrace();
            getLogger().error("Failed to read sparql query.");
        }

        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                stmts.get().forEach(stmt -> processStmts(conn, stmt, out));
            }
        });
        if (contentFlowFile.equals(FLOW_FILE_CONTENTS.NON_RDF_DATA)) {
            try {
                System.out.println("! Deleting File From The Configured Path !");
                Files.delete(path);
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
        session.transfer(flowFile, SUCCESS);
    }

    public static Stream<SparqlStmt> parseSparqlQuery(String str, Function<String, SparqlStmt> parser)
            throws IOException, ParseException {
        Stream<SparqlStmt> result = Streams.stream(new SparqlStmtIterator(parser, str));
        return result;
    }

    public static void processStmts(RDFConnection conn, SparqlStmt stmt, OutputStream out) {
        // logger.info("Processing SPARQL Statement: " + stmt);

        if (stmt.isQuery()) {
            SparqlStmtQuery qs = stmt.getAsQueryStmt();
            Query q = qs.getQuery();
            q.isConstructType();
            conn.begin(ReadWrite.READ);
            // SELECT -> STDERR, CONSTRUCT -> STDOUT
            QueryExecution qe = conn.query(q);

            if (q.isConstructQuad()) {
                // ResultSetFormatter.ntrqe.execConstructTriples();
                // throw new RuntimeException("not supported yet");
                SinkQuadOutput sink = new SinkQuadOutput(out, null, null);
                Iterator<Quad> it = qe.execConstructQuads();
                while (it.hasNext()) {
                    Quad t = it.next();
                    sink.send(t);
                }
                sink.flush();
                sink.close();

            } else if (q.isConstructType()) {
                // System.out.println(Algebra.compile(q));

                SinkTripleOutput sink = new SinkTripleOutput(out, null, null);
                Iterator<Triple> it = qe.execConstructTriples();
                while (it.hasNext()) {
                    Triple t = it.next();
                    sink.send(t);
                }
                sink.flush();
                sink.close();
            } else if (q.isSelectType()) {
                ResultSet rs = qe.execSelect();
                String str = ResultSetFormatter.asText(rs);
                System.err.println(str);
            } else {
                throw new RuntimeException("Unsupported query type");
            }

            conn.end();
        } else if (stmt.isUpdateRequest()) {
            UpdateRequest u = stmt.getAsUpdateStmt().getUpdateRequest();

            conn.update(u);
        }
    }
}
