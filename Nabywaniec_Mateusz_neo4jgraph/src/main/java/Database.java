import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.File;

public class Database {
    private final GraphDatabaseService graphDatabaseService;

    private static final String GRAPH_DIR_LOC = "C:\\Users\\Mateusz\\.Neo4jDesktop\\neo4jDatabases\\database-99a8b726-4771-41d7-bbc4-62f2a5b6c66a\\installation-3.5.12\\data\\databases\\graph.db\n";
    public static Database createDatabase() {
        return new Database();
    }

    private Database() {
        graphDatabaseService = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(new File(GRAPH_DIR_LOC))
                .setConfig(GraphDatabaseSettings.allow_upgrade, "true")
                .newGraphDatabase();
    }

    public GraphDatabaseService getGraphDatabaseService() {
        return graphDatabaseService;
    }

    public String runQuery(final String query) {
        try (Transaction transaction = graphDatabaseService.beginTx()) {
            final Result result = graphDatabaseService.execute(query);
            transaction.success();
            return result.resultAsString();
        }
    }
}
