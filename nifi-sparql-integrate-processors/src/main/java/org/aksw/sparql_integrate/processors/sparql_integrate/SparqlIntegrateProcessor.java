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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;
import com.google.common.collect.Streams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.aksw.jena_sparql_api.sparql.ext.http.JenaExtensionHttp;
import org.aksw.jena_sparql_api.sparql.ext.util.JenaExtensionUtil;
import org.aksw.jena_sparql_api.stmt.SPARQLResultSink;
import org.aksw.jena_sparql_api.stmt.SPARQLResultSinkQuads;
import org.aksw.jena_sparql_api.stmt.SparqlStmt;
import org.aksw.jena_sparql_api.stmt.SparqlStmtIterator;
import org.aksw.jena_sparql_api.stmt.SparqlStmtParser;
import org.aksw.jena_sparql_api.stmt.SparqlStmtParserImpl;
import org.aksw.jena_sparql_api.stmt.SparqlStmtUtils;
import org.aksw.sparql_integrate.cli.SPARQLResultVisitorSelectJsonOutput;
import org.aksw.sparql_integrate.cli.SparqlStmtProcessor;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jena.atlas.lib.Sink;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.shared.impl.PrefixMappingImpl;
import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.sparql.core.Quad;
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
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import io.github.galbiston.geosparql_jena.configuration.GeoSPARQLConfig;

@Tags({"RDF", "SPARQL"})
@CapabilityDescription("This processor takes an SPARQL query as an argument and outputs a RDF-Turtle file.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "mime.type", description = "Identify what kind of Input is in FlowFile")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
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

    public static final AllowableValue TURTLE = new AllowableValue(Lang.TURTLE.getLabel());
    public static final AllowableValue NT = new AllowableValue(Lang.NT.getLabel());
    public static final AllowableValue JSONLD = new AllowableValue(Lang.JSONLD.getLabel());

    public static final PropertyDescriptor BASE_URI = new PropertyDescriptor.Builder()
            .name("BASE_URI")
            .displayName("Base URI")
            .description("Base URI for the SPARQL queries.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor SPARQL_QUERY_PROPERTY = new PropertyDescriptor.Builder()
            .name("SPARQL_QUERY")
            .displayName("SPARQL Query")
            .description("The SPARQL query to run.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor RDF_DATA_INPUT_SYNTAX = new PropertyDescriptor.Builder()
            .name("RDF_DATA_INPUT_SYNTAX")
            .displayName("RDF Data Input Syntax")
            .description("RDF-Syntax of the FlowFile content, only used when Content of flow file is set to rdf-data")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .allowableValues(TURTLE, NT, JSONLD)
            .defaultValue(TURTLE.getValue())
            .build();

    public static final PropertyDescriptor CONTENT_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("CONTENT_FLOW_FILE")
            .displayName("Content of FlowFile")
            .description("Content of the processors input FlowFile")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .allowableValues(RDF_DATA, SPARQL_QUERY, NON_RDF_DATA, EMPTY)
            .defaultValue(RDF_DATA.getValue())
            .build();

    public static final PropertyDescriptor FORMAT_OUTPUT = new PropertyDescriptor.Builder()
            .name("FORMAT_OUTPUT")
            .displayName("Format of outout FlowFile")
            .description("Format of outout FlowFile")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .allowableValues(NT, JSONLD)
            .defaultValue(NT.getValue())
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
        descriptors.add(RDF_DATA_INPUT_SYNTAX);
        descriptors.add(SPARQL_QUERY_PROPERTY);
        descriptors.add(FORMAT_OUTPUT);
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

        final ComponentLog logger = getLogger();
        final String contentFlowFile = context.getProperty(CONTENT_FLOW_FILE).getValue();
        final String formatOutput = context.getProperty(FORMAT_OUTPUT).getValue();
        final String rdfDataInputSyntax = context.getProperty(RDF_DATA_INPUT_SYNTAX).getValue();
        final AtomicReference<Stream<SparqlStmt>> stmts = new AtomicReference<>();
        // Disable creation of a derby.log file ; triggered by the GeoSPARQL module
        System.setProperty("derby.stream.error.field", "org.aksw.sparql_integrate.cli.DerbyUtil.DEV_NULL");
        // Init geosparql module
        GeoSPARQLConfig.setupNoIndex();

        FlowFile flowFile = session.get();
        String baseUri = context.getProperty(BASE_URI).isSet()
                ? context.getProperty(BASE_URI).evaluateAttributeExpressions().getValue()
                : "";
        Path path = null;
        Dataset dataset = DatasetFactory.create();
        String sparqlQuery = null;
        switch (contentFlowFile) {
            case FLOW_FILE_CONTENTS.RDF_DATA:
                sparqlQuery =
                        context.getProperty(SPARQL_QUERY_PROPERTY).evaluateAttributeExpressions(flowFile).getValue();
                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(InputStream in) throws IOException {
                        RDFDataMgr.read(dataset, in, RDFLanguages.nameToLang(rdfDataInputSyntax));
                    }
                });
                break;
            case FLOW_FILE_CONTENTS.NON_RDF_DATA:
                sparqlQuery =
                        context.getProperty(SPARQL_QUERY_PROPERTY).evaluateAttributeExpressions(flowFile).getValue();
                path = new File("/tmp/" + flowFile.getAttribute("filename")).toPath();
                baseUri = path.toAbsolutePath().getParent().toString() + "/";
                session.exportTo(flowFile, path, false);
                break;
            case FLOW_FILE_CONTENTS.SPARQL_QUERY:
                final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                session.exportTo(flowFile, bytes);
                sparqlQuery = bytes.toString();
                break;
        }

        logger.info("SparqlQuery: " + sparqlQuery);
        logger.info("BaseUri: " + baseUri.toString());

        RDFConnection conn = RDFConnectionFactory.connect(dataset);
        PrefixMapping pm = new PrefixMappingImpl();
        pm.setNsPrefixes(PrefixMapping.Extended);
        JenaExtensionUtil.addPrefixes(pm);
        JenaExtensionHttp.addPrefixes(pm);
        Prologue prologue = new Prologue();
        prologue.setPrefixMapping(pm);
        prologue.setBaseURI(baseUri);
        Function<String, SparqlStmt> rawSparqlStmtParser =
                SparqlStmtParserImpl.create(Syntax.syntaxARQ, prologue, true);
        SparqlStmtParser sparqlStmtParser = SparqlStmtParser.wrapWithNamespaceTracking(pm, rawSparqlStmtParser);

        SparqlStmtIterator stmtIter;
        try {
            stmtIter = SparqlStmtUtils.parse(IOUtils.toInputStream(sparqlQuery, "UTF-8"), sparqlStmtParser);
            stmts.set(Streams.stream(stmtIter));
        } catch (Exception e) {
            e.printStackTrace();
        }
        SparqlStmtProcessor processor = new SparqlStmtProcessor();
        processor.setShowQuery(true);
        // SPARQLResultSink sink;
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                SPARQLResultSink sink;
                if (formatOutput.equals(NT.getValue())) {
                    Sink<Quad> quadSink = SparqlStmtUtils.createSink(RDFFormat.NT, out, pm);
                    sink = new SPARQLResultSinkQuads(quadSink);
                } else {
                    Gson gson = new GsonBuilder().setPrettyPrinting().create();
                    PrintStream printOut = new PrintStream(out);
                    sink = new SPARQLResultVisitorSelectJsonOutput(null, 4, true, gson, printOut, printOut);
                }
                stmts.get().forEach(stmt -> processor.processSparqlStmt(conn, stmt, sink));
                sink.flush();
                try {
                    sink.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        if (path != null) {
            try {
                System.out.println("! Deleting File From The Configured Path !");
                Files.delete(path);
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
        String filename = FilenameUtils.getBaseName(flowFile.getAttribute("filename")) + ".nt";
        session.putAttribute(flowFile, "filename", filename);
        session.transfer(flowFile, SUCCESS);
    }
}
