package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

class Metadata {
    private List<Column> columns;
    private List<Row> rows;
    private Integer offset;
    private Integer totalLength;

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public List<Row> getRows() {
        return rows;
    }

    public void setRows(List<Row> rows) {
        this.rows = rows;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public Integer getTotalLength() {
        return totalLength;
    }

    public void setTotalLength(Integer totalLength) {
        this.totalLength = totalLength;
    }

    @Override
    protected Metadata clone() {
        var copy = new Metadata();
        var columnCopy = new ArrayList<Column>();
        for (Column column : columns) {
            columnCopy.add(column.clone());
        }
        var rowCopy = new ArrayList<Row>();
        for (Row row : rows) {
            rowCopy.add(row.clone());
        }
        copy.setColumns(columnCopy);
        copy.setRows(rowCopy);
        copy.setOffset(offset);
        copy.setTotalLength(totalLength);
        return copy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Metadata metadata = (Metadata) o;
        return Objects.equals(columns, metadata.columns) &&
                Objects.equals(rows, metadata.rows) &&
                Objects.equals(offset, metadata.offset) &&
                Objects.equals(totalLength, metadata.totalLength);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, rows, offset, totalLength);
    }

    public static class Column {
        private String name;
        private String label;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        @Override
        protected Column clone() {
            var copy = new Column();
            copy.setName(name);
            copy.setLabel(label);
            return copy;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Column column = (Column) o;
            return Objects.equals(name, column.name) &&
                    Objects.equals(label, column.label);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, label);
        }
    }

    public static class Row {
        private Map<String, Object> properties;
        private List<String> modifiedProperties;
        @JsonProperty("@class")
        private String clazz;

        public Map<String, Object> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, Object> properties) {
            this.properties = properties;
        }

        public List<String> getModifiedProperties() {
            return modifiedProperties;
        }

        public void setModifiedProperties(List<String> modifiedProperties) {
            this.modifiedProperties = modifiedProperties;
        }

        public String getClazz() {
            return clazz;
        }

        public void setClazz(String clazz) {
            this.clazz = clazz;
        }

        @Override
        protected Metadata.Row clone() {
            var copy = new Row();
            copy.setProperties(new HashMap<>(properties));
            copy.setModifiedProperties(new ArrayList<>(modifiedProperties));
            copy.setClazz(clazz);
            return copy;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Row row = (Row) o;
            return Objects.equals(properties, row.properties) &&
                    Objects.equals(modifiedProperties, row.modifiedProperties) &&
                    Objects.equals(clazz, row.clazz);
        }

        @Override
        public int hashCode() {
            return Objects.hash(properties, modifiedProperties, clazz);
        }
    }

}
