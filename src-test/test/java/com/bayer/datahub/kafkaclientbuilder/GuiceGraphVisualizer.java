package com.bayer.datahub.kafkaclientbuilder;

import com.bayer.datahub.libs.config.ConfigFileLoader;
import com.google.inject.Guice;
import com.google.inject.grapher.graphviz.GraphvizGrapher;
import com.google.inject.grapher.graphviz.GraphvizModule;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;

import static com.bayer.datahub.ResourceHelper.resourceToFile;

/**
 * Print Guice modules tree to *.dot file (view by Graphviz).
 */
public class GuiceGraphVisualizer {
    public static void main(String[] args) throws IOException {
        var graphvizInjector = Guice.createInjector(new GraphvizModule());
        var outputFile = Files.createTempFile(GuiceGraphVisualizer.class.getSimpleName(), ".dot");

        var out = new PrintWriter(outputFile.toFile());

        var grapher = graphvizInjector.getInstance(GraphvizGrapher.class);
        grapher.setOut(out);
        grapher.setRankdir("TB");

        var configFile = resourceToFile(GuiceGraphVisualizer.class, "GuiceGraphVisualizer.properties");
        var configs = ConfigFileLoader.readConfigsListFromFile(configFile).get(0);
        var inspectedInjector = Guice.createInjector(new AppModule(configs));
        grapher.graph(inspectedInjector);

        System.out.println("Guice graph diagram: " + outputFile);
    }
}
