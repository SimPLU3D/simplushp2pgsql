import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.log4j.Logger;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Level;

import fr.ign.cogit.geoxygene.api.feature.IFeature;
import fr.ign.cogit.geoxygene.api.feature.IPopulation;
import fr.ign.cogit.geoxygene.feature.DefaultFeature;
import fr.ign.cogit.geoxygene.sig3d.io.vector.PostgisManager;
import fr.ign.cogit.geoxygene.util.attribute.AttributeManager;
import fr.ign.cogit.geoxygene.util.conversion.ShapefileReader;

public class Simplushp2pgsql {

	// nested class to avoid some verbosity
	// really a glorified struct..
	
	private static boolean tableExists;
	private static class Credential {
		private String host;
		private String db;
		private String schema;
		private String port;
		private String user;
		private String password;
		private String table;
		private String shapesDir;
		private boolean createTable;

		private Credential(String host, String db, String schema, String port, String user, String password,
				String table, String dirPath, boolean createtable) {
			this.host = host;
			this.db = db;
			this.schema = schema;
			this.port = port;
			this.user = user;
			this.password = password;
			this.table = table;
			this.shapesDir = dirPath;
			this.createTable = createtable;
		}

		public static Credential makeCredential(String dbCreds) {
			Properties creds = new Properties();
			Reader reader;
			try {
				reader = new FileReader(dbCreds);
				creds.load(reader);
			} catch (IOException e) {
				e.printStackTrace();
			}
			boolean createTable = creds.getProperty("createTable").equals("true");
			tableExists = !createTable;
			return new Credential(creds.getProperty("host"), creds.getProperty("db"), creds.getProperty("schema"),
					creds.getProperty("port"), creds.getProperty("user"), creds.getProperty("password"),
					creds.getProperty("table"), creds.getProperty("shapesDir"), createTable);
		}
	}

	/**
	 * @param dirPath
	 *            directory containing all the results subdirectories for a
	 *            simulation
	 * @return a list of Path corresponding to all subdirectories of dirPath
	 */
	public static List<Path> getSubdirs(String dirPath) {
		Path dir = Paths.get(dirPath);
		DirectoryStream<Path> stream = null;
		try {
			stream = Files.newDirectoryStream(dir, p -> Files.isDirectory(p));
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<Path> dirs = new ArrayList<>();
		for (Path p : stream) {
			dirs.add(p);
		}
		return dirs;
	}

	/**
	 * check if the run identifier is correctly formated as YYMMDD
	 * 
	 * @param run
	 * @return
	 */
	public static boolean formatIsOk(String run) {
		if (run.length() != 6 && !NumberUtils.isDigits(run))
			return false;
		SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd");
		sdf.setLenient(false);
		try {
			sdf.parse(run);
		} catch (ParseException e) {
			System.out.println("bad format");
			return false;
		}
		return true;
	}

	/**
	 * Write the shapefiles in dirPath with the 'run' identifier in the database
	 * referenced by creds
	 * 
	 * @param dirPath
	 * @param run
	 * @param creds
	 */
	public static void writeShapetoDB(Path dirPath, String run, Credential creds) {
		String imu = "" + dirPath.getFileName();
		List<String> dirs = new ArrayList<>();
		//boolean tableExists = !creds.createTable;
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirPath, "*.shp")) {
			for (Path p : stream) {
				dirs.add(p.toString());
				IPopulation<IFeature> shapes = ShapefileReader.read(p.toString());
				for (IFeature f : shapes) {
					DefaultFeature df = (DefaultFeature) f;
					AttributeManager.addAttribute(df, "directory", imu, "String");
					AttributeManager.addAttribute(df, "run", run, "String");
					// System.out.println(df.getGeom());
				}
				// System.out.println(p + " -> " + imu + " : " + shapes.size() +
				// " features");
				if (tableExists) {
					// PostgisManager.insertInGeometricTable(creds.host"localhost",
					// "5432", "iudf2", "imrandb", "imrandb", "asd", shapes);
					PostgisManager.insertInGeometricTable(creds.host, creds.port, creds.db, creds.table, creds.user, creds.password,
							 shapes);
				} else {
					// PostgisManager.saveGeometricTable("localhost", "5432",
					// "iudf2", "imrandb", "imrandb", "asd", shapes, false);
					PostgisManager.saveGeometricTable(creds.host, creds.port, creds.db, creds.schema, creds.table, creds.user, creds.password,
							 shapes, false);
					tableExists = true;
				}
				// System.out.println(shapes);
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		Logger logger = Logger.getLogger(fr.ign.cogit.geoxygene.util.conversion.Reader.class.getName());
		logger.setLevel(Level.OFF);

		PostgisManager.NAME_COLUMN_GEOM = "geom";
		PostgisManager.setLoggerLevel(Level.OFF);
		if (args.length != 2) {
			System.out.println(
					"Proper Usage is:\n simplushp2pgsql conf run\nWith conf being the path to the credentials file and run like YYMMDD\nExample :\n simplushp2pgsql /home/operator/creds.conf 170518");
			System.exit(0);
		}
		String dbCreds = "/home/imran/credsimplu.conf";
		dbCreds = args[0];
		// Credential conf = getDbSrcTarg(dbCreds);
		Credential conf = Credential.makeCredential(dbCreds);
		String run = "170518";
		run = args[1];
		if (!formatIsOk(run)) {
			System.out.println("incorrect format for run : it should be as YYMMDD like 170518");
			System.exit(0);
		}

		List<Path> ll = getSubdirs(conf.shapesDir);
		long beg = System.currentTimeMillis();
		long end = 0;
		System.out.println("Number of IMUs (dirs) to write : " + ll.size());
		int i = 0;
		for (Path dir : ll) {
			writeShapetoDB(dir, run, conf);
			++i;
			if (i % 300 == 0) {
				end = System.currentTimeMillis() - beg;
				System.out.format("%06d IMUS treated so far in %.2f minutes \n", i, end * 1.0 / 60000);
			}
		}
		end = System.currentTimeMillis() - beg;
		System.out.format("Import done in %.2f minutes \n", end * 1.0 / 60000);
	}

}
