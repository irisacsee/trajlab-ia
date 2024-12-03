package org.irisacsee.trajlab.conf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.irisacsee.trajlab.conf.enums.FileMode;
import org.irisacsee.trajlab.conf.enums.FileType;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class StandaloneLoadConfig implements ILoadConfig {
    private String master;
    private String location;
    private FileMode fileModeEnum;
    private int partNum;
    private String splitter;
    private FileType fileType;
    private String filterText;

  @JsonCreator
  public StandaloneLoadConfig(
      @JsonProperty("master") String master,
      @JsonProperty("location") String location,
      @JsonProperty("fileMode") FileMode fileModeEnum,
      @JsonProperty("partNum") @JsonInclude(JsonInclude.Include.NON_NULL) int partNum,
      @JsonProperty("splitter") String splitter,
      @JsonProperty("fileType") FileType fileType,
      @JsonProperty("filterText") String filterText) {
        this.master = master;
        this.location = location;
        this.fileModeEnum = fileModeEnum;
        this.partNum = partNum;
        this.splitter = splitter;
        this.fileType = fileType;
        this.filterText = filterText;
    }

    public String getMaster() {
        return this.master;
    }

    public FileType getFileType() {
        return fileType;
    }

    public int getPartNum() {
        return this.partNum == 0 ? 1 : this.partNum;
    }

    public String getFilterText() {
        return filterText;
    }

    public FileMode getFileModeEnum() {
        return fileModeEnum;
    }

    public String getLocation() {
        return this.location;
    }

    public FileMode getFileMode() {
        return this.fileModeEnum;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public ILoadConfig.InputType getInputType() {
        return InputType.STANDALONE;
    }

    @Override
    public String getFsDefaultName() {
        return null;
    }

    public String getSplitter() {
        return this.splitter;
    }


}