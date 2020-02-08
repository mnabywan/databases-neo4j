import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class Solution {
    private final Database graphDatabase = Database.createDatabase();



    public void runTests(){
        databaseStatistics();
        System.out.println(findMovieByTitle("The Green Mile"));
        //System.out.println(findMoviesByDirector("Clint Eastwood"));

/*
        final String newMovie = "Psy";
        final String newMovie2 = "Psy 2: Ostatnia krew";

        final String newActor = "Boguslaw Linda";
        final String newActor2 = "Cezary Pazura";
        final String newActor3 = "Artur Zmijewski";
        System.out.println(createActorWithMovie(newActor, newMovie));
        System.out.println(createActorWithMovie(newActor2, newMovie));
        System.out.println(createActorWithMovie(newActor, newMovie2));
        System.out.println(createActorWithMovie(newActor2, newMovie2));
        System.out.println(createActorWithMovie(newActor3, newMovie2));
*/

        //final String actor = "Janusz Gajos";
        //final String actor2 = "Olaf Lubaszenko";

/*
        final String movie = "Pilkarski poker";

        System.out.println(createActor(actor));
        System.out.println(createActor(actor2));
        System.out.println(createMovie(movie));

        System.out.println(createActedInRelationship(actor, movie));
        System.out.println(createActedInRelationship(actor2, movie));
*/
/*
        final String birthPlace1 = "Dabrowa Gornicza";
        final String birthPlace2 = "Wroclaw";

        final int birthdate1 = 1939;
        final int birthdate2 = 1968;

        System.out.println(setBirthInfo(actor, birthPlace1, birthdate1));
        System.out.println(setBirthInfo(actor2, birthPlace2, birthdate2));
        System.out.println(changeAttributeOfNode("Movie", "title", "Pilkarski poker", "subtitle", "Film nagrodzony Zlotym Orlem"));
        System.out.println(findActorsWhoPlayedInManyMovies(2));
        System.out.println(findAverageAmountOfMovies(3));
        Node node1;
        try (Transaction tx= graphDatabase.getGraphDatabaseService().beginTx()) {
            node1 = getNode();
            // do your stuff
            tx.success();
        }
        System.out.println(node1.toString());
        System.out.println(changeAttributesInPath(36, 18 ,3, "marked", "true"));
        System.out.println(displaySecondNode(36,18));
        calculateSearchingTime("Kevin Bacon");
*/
        //this.optimalizedFindMoviesByDirector("Clint Eastwood");
        //this.optimalizedFindActorsWhoPlayedInManyMovies(4);
        //this.createNewNodes();
    }


    private void calculateSearchingTime(String actorName){
        long start,end;
        String query = "MATCH (p:Person {name: \"%s\"}) RETURN p";
        graphDatabase.runQuery("CREATE INDEX ON :Person(name)");
        start = System.nanoTime();
        graphDatabase.runQuery(String.format(query, actorName));
        end = System.nanoTime();
        System.out.println();
        System.out.println(String.format("With index:\t\t68567 us", (end - start ) / 1000 ));


        graphDatabase.runQuery("DROP INDEX ON :Person(name)");
        start = System.nanoTime();
        graphDatabase.runQuery(String.format(query, actorName));
        end = System.nanoTime();
        System.out.println(String.format("Without index:\t\t%7d us", (end - start) / 1000));

    }


    private String displaySecondNode(int id1, int id2){
        String query = "MATCH path = (a)-- (b)- [*3]-(c) " +
                "WHERE ID(a)= %d  AND ID(c) = %d " +
                "RETURN b";
        return graphDatabase.runQuery(String.format(query, id1, id2));
    }

    private String changeAttributesInPath(int id1, int id2, int maxLen, String attribute, String value){
        String query = "MATCH path = (a)-[*..%d]-(b) " +
                "WHERE ID(a)= %d  AND ID(b) = %d " +
                "FOREACH (n IN nodes(path) | SET n.%s = \"%s\" ) " +
                "RETURN nodes(path)";
        return graphDatabase.runQuery(String.format(query, maxLen, id1, id2, attribute, value));
    }


    private void optimalizedFindActorsWhoPlayedInManyMovies(int moviesAmount){
        long start,end;
        graphDatabase.runQuery("CREATE INDEX ON :Person(name)");
        graphDatabase.runQuery("CREATE INDEX ON :Movie(title)");
        String query = "MATCH (p:Person)-[:ACTED_IN]->(m:Movie) " +
                "WITH p, collect(m.title) as movies " +
                "WHERE length(movies) >= %d " +
                "RETURN p.name as name";

        start = System.nanoTime();
        System.out.println("Find actors ho played in " + moviesAmount + " or more movies");
        System.out.println(graphDatabase.runQuery(String.format(query, moviesAmount)));
        end = System.nanoTime();
        System.out.println(String.format("Time with index = \t\t%7d us ", (end - start)/1000));
        System.out.println();

        graphDatabase.runQuery("DROP INDEX ON :Person(name)");
        graphDatabase.runQuery("DROP INDEX ON :Movie(title)");
        start = System.nanoTime();
        System.out.println("Find actors who played in " + moviesAmount + " or more movies");
        System.out.println(graphDatabase.runQuery(String.format(query, moviesAmount)));
        end = System.nanoTime();
        System.out.println(String.format("Time without index = \t\t%7d us ", (end - start)/1000 ));
    }



    private Node getNode(){
        return graphDatabase.getGraphDatabaseService().getNodeById(12);
    }

    private String findAverageAmountOfMovies(int moviesAmount){
        String query = "MATCH (p:Person)-[:ACTED_IN]->(m:Movie) " +
                "WITH p, collect(m.title) as movies " +
                "WHERE length(movies) >= %d " +
                "RETURN avg(length(movies)) as average_number_of_movies";
        return graphDatabase.runQuery(String.format(query, moviesAmount));
    }

    private String changeAttributeOfNode(String type, String otherAttribute, String otherAttributeValue, String attributeToSet, String attributeToSetValue){
        String query = "MATCH (a: %s {%s : \"%s\"}) " +
                "SET  a.%s  = \"%s\" " +
                "RETURN *";
        return graphDatabase.runQuery(String.format(query, type, otherAttribute, otherAttributeValue, attributeToSet, attributeToSetValue));

    }


    private String setBirthInfo(String actor, String birthPlace, int birthDate){
        String query = "MATCH (a:Actor) \n" +
                "WHERE a.name = \"%s\" \n" +
                "SET a.birthplace = \"%s\", a.birthdate = \"%d\"" +
                "RETURN a.name, a.birthplace, a.birthdate";
        return graphDatabase.runQuery(String.format(query, actor, birthPlace, birthDate));
    }



    private String createActor(String actorName){
        String query = "CREATE (a:Actor {name: \"%s\"})";
        return graphDatabase.runQuery(String.format(query,actorName ));
    }

    private String createMovie(String movieTitle){
        String query = "CREATE(m:Movie {title: \"%s\"})";
        return graphDatabase.runQuery(String.format(query, movieTitle ));
    }

    private String createActedInRelationship(String actorName, String movieTitle){
        String query = "MATCH (a:Actor),(m:Movie)\n" +
                "WHERE a.name = \"%s\" AND m.title = \"%s\" \n" +
                "CREATE (a)-[r:ACTED_IN]->(m)\n" +
                "RETURN type(r)";
        return graphDatabase.runQuery(String.format(query, actorName, movieTitle ));
    }


    private void databaseStatistics() {
        System.out.println(graphDatabase.runQuery("CALL db.labels()"));
        System.out.println(graphDatabase.runQuery("CALL db.relationshipTypes()"));
    }

    private String findMovieByTitle(final String movieTitle) {
        String query = "PROFILE MATCH (m:Movie) WHERE m.title CONTAINS \"%s\"" +
                " RETURN m.title as title, m.released as release_year" +
                " LIMIT 1";
        return graphDatabase.runQuery(String.format(query, movieTitle));
    }

    private void optimalizedFindMoviesByDirector(final String directorName) {
        long start,end;
        graphDatabase.runQuery("CREATE INDEX ON :Person(name)");
        graphDatabase.runQuery("CREATE INDEX ON :Movie(title)");
        String query = "MATCH (m:Movie)-[:DIRECTED]-(p:Person {name : \"%s\" })\n" +
                "RETURN m.title as title, p.name as director_name LIMIT 20";

        start = System.nanoTime();
        System.out.println("FIND MOVIES BY DIRECTOR: " + directorName);
        System.out.println(graphDatabase.runQuery(String.format(query, directorName)));
        end = System.nanoTime();
        System.out.println(String.format("Time with index = \t\t%7d us ", (end - start)/1000 ));

        graphDatabase.runQuery("DROP INDEX ON :Person(name)");
        graphDatabase.runQuery("DROP INDEX ON :Movie(title)");
        start = System.nanoTime();
        System.out.println("FIND MOVIES BY DIRECTOR: " + directorName);
        System.out.println(graphDatabase.runQuery(String.format(query, directorName)));
        end = System.nanoTime();
        System.out.println(String.format("Time without index = \t\t%7d us ", (end - start)/1000 ));

    }



    private String findActorByName(final String actorName) {
        return graphDatabase.runQuery(
                String.format("MATCH (p:Actor {name: \"%s\"}) return p", actorName)
        );
    }


    private void findCommonMoviesForActors(String actorOne, String actorTwo) {
        long start,end;
        graphDatabase.runQuery("CREATE INDEX ON :Person(name)");
        graphDatabase.runQuery("CREATE INDEX ON :Movie(title)");
        String query = "MATCH (a1:Person {name: \"%s\"}) -[:ACTED_IN]- (m:Movie) -[:ACTED_IN]- (a2:Person {name: \"%s\"}) RETURN m.title";

        start = System.nanoTime();
        System.out.println("Common movies for" + actorOne + ", " + actorTwo);
        System.out.println(graphDatabase.runQuery(String.format(query, actorOne, actorTwo)));
        end = System.nanoTime();
        System.out.println(String.format("Time with index = \t\t%7d us ", (end - start)/1000 ));

        graphDatabase.runQuery("DROP INDEX ON :Person(name)");
        graphDatabase.runQuery("DROP INDEX ON :Movie(title)");
        start = System.nanoTime();
        System.out.println("Common movies for" + actorOne + ", " + actorTwo);
        System.out.println(graphDatabase.runQuery(String.format(query, actorOne, actorTwo)));
        end = System.nanoTime();
        System.out.println(String.format("Time without index = \t\t%7d us ", (end - start)/1000 ));
    }

    private String createActorWithMovie(final String actorName, final String movieTitle) {
        String query = "CREATE (a:Actor {name: \"%s\"}) -[:ACTED_IN]-> (m:Movie {title: \"%s\"})";
        return graphDatabase.runQuery(String.format(query, actorName, movieTitle));
    }



}
