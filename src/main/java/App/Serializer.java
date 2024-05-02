package App;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import Exceptions.DBAppException;

public class Serializer {
    private static FileOutputStream fileOutput;
    private static ObjectOutputStream objectOutput;
    private static FileInputStream fileInput;
    private static ObjectInputStream objectInput;

    public static void serializeTable(Table table) throws IOException {
        // serializes table using the path format we created.
        // TableFolders -> Table A -> Table A.ser ,  0.ser,  1.ser and so on
    	
        String path = getFilePath(table.getTableName(), table.getTableName());
        fileOutput = new FileOutputStream(path);
        objectOutput = new ObjectOutputStream(fileOutput);

        objectOutput.writeObject(table);

        objectOutput.close();
        fileOutput.close();
    }

    public static Table deserializeTable(String tableName) throws IOException, ClassNotFoundException {
        String path = getFilePath(tableName, tableName);
        fileInput = new FileInputStream(path);
        objectInput = new ObjectInputStream(fileInput);

        Table table = (Table) objectInput.readObject();

        objectInput.close();
        fileInput.close();

        return table;
    }

    public static void serializePage(String pageNum, Page page) throws IOException {
        String path = getFilePath(page.getTableName(), pageNum);
        fileOutput = new FileOutputStream(path);
        objectOutput = new ObjectOutputStream(fileOutput);

        objectOutput.writeObject(page);

        objectOutput.close();
        fileOutput.close();

    }

    public static Page deserializePage(String tableName, String pageNum) throws IOException, ClassNotFoundException {
        String path = getFilePath(tableName, pageNum);
        fileInput = new FileInputStream(path);
        objectInput = new ObjectInputStream(fileInput);

        Page page = (Page) objectInput.readObject();

        objectInput.close();
        fileInput.close();

        return page;
    }

    public static String getFilePath(String tableName, String pageName) {
        return "TableFolders//" + tableName + "//" + pageName + ".ser";

    }
}
