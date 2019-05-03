package com.ongres.benchmark.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Version {
  
  private static final Singleton SINGLETON = new Singleton();
  
  private static class Singleton {
    private final Properties properties = getProperties();
    
    private Properties getProperties() {
      try {
        Properties properties = new Properties();
        try (InputStream inputStream = Version.class.getResourceAsStream("/project.properties")) {
          properties.load(inputStream);
        }
        return properties;
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  public static String getVersion() throws IOException {
    return SINGLETON.properties.getProperty("version");
  }

}