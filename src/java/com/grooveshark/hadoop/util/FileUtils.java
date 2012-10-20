package com.grooveshark.hadoop.util;

import java.util.List;
import java.util.LinkedList;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class FileUtils
{

    public static List<String> readFile(String filename)
        throws Exception
    {
        BufferedReader br = new BufferedReader(new FileReader(filename));
        List<String> lines = new LinkedList<String>();
        String line = "";
        while ((line = br.readLine()) != null) {
            lines.add(line);
        }
        return lines;
    }
}
