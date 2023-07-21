package org.ds.dsv2;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Optional;

public class DatasetOptions implements Serializable{

    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetOptions.class);

    private String inPath;

    private String outPath;

    private int nbPartitions;

    public DatasetOptions(
            String inPath,
            String outPath,
            Optional<String> nbPartitions
            ) {
        this.nbPartitions = Integer.parseInt(nbPartitions.orElse("0"));
        this.inPath = inPath;
        this.outPath = outPath;
    }

    public String getInPath() {
        return inPath;
    }

    public String getOutPath() {
        return outPath;
    }

    public int getNbPartitions() {
        return nbPartitions;
    }



}
