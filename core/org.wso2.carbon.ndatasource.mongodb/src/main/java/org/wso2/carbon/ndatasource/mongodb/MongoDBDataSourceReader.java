package org.wso2.carbon.ndatasource.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.wso2.carbon.ndatasource.common.DataSourceException;
import org.wso2.carbon.utils.CarbonUtils;

import javax.xml.bind.JAXBContext;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import java.io.StringReader;

public class MongoDBDataSourceReader implements MongoDBDataSourceReaderDAO{
    @Override
    public String getType() {
        return "NosSQL";
    }


    public static MongoDBConfiguration loadConfig(String xmlConfiguration)
            throws DataSourceException {
        try {
            xmlConfiguration = CarbonUtils.replaceSystemVariablesInXml(xmlConfiguration);
            JAXBContext ctx = JAXBContext.newInstance(MongoDBConfiguration.class);
            XMLInputFactory inputFactory = XMLInputFactory.newInstance();
            inputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
            inputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
            XMLStreamReader xmlReader = inputFactory.createXMLStreamReader(new StringReader(xmlConfiguration));

            return (MongoDBConfiguration) ctx.createUnmarshaller().unmarshal(xmlReader);
        } catch (Exception e) {
            throw new DataSourceException("Error in loading RDBMS configuration: " +
                    e.getMessage(), e);
        }
    }

    @Override
    public Object createDataSource(String xmlConfiguration, boolean isDataSourceFactoryReference)
            throws DataSourceException {
        MongoDBConfiguration mongoConfig = loadConfig(xmlConfiguration);
        if (mongoConfig != null && mongoConfig.getUrl() != null &&
                (mongoConfig.getUrl().toLowerCase().contains(";init="))) {
            throw new DataSourceException(
                    "INIT expressions are not allowed in the connection URL due to security reasons.");
        }

        //todo: pooling
//        if (isDataSourceFactoryReference) {
//            return (new RDBMSDataSource(mongoConfig).getDataSourceFactoryReference());
//        } else {
//            return (new RDBMSDataSource(mongoConfig).getDataSource());
//        }
        return mongoConfig;
    }

    @Override
    public boolean testDataSourceConnection(String xmlConfiguration) throws DataSourceException {
        final String DATABASE_NAME = "test";
        final String COLLECTION_NAME = "col1";
        final String CONNECTION_STRING = "mongodb://localhost:27017/";

        MongoClientURI uri = new MongoClientURI(CONNECTION_STRING);
        MongoClient client = new MongoClient(uri);
        try{
            MongoDatabase database = client.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

            for (Document doc : collection.find()){
                System.out.println(doc.toJson());
            }
        }
        catch(Error e){
            throw new DataSourceException("The user is not associated with a trusted SQL Server connection." + e.getMessage(), e);
        }
        finally{
            client.close();
        }

        return true;
    }

//    public static void main(String[] args){
//        MongoClientURI uri = new MongoClientURI(CONNECTION_STRING);
//        MongoClient client = new MongoClient(uri);
//        MongoDatabase database = client.getDatabase(DATABASE_NAME);
//        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);
//
//        for (Document doc : collection.find()){
//            System.out.println(doc.toJson());
//        }
//
//        client.close();
//    }

}
