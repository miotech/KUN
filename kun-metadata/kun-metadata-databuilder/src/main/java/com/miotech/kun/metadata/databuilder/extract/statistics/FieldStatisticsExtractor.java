package com.miotech.kun.metadata.databuilder.extract.statistics;

import com.miotech.kun.metadata.core.model.dataset.Dataset;
import com.miotech.kun.metadata.core.model.dataset.FieldStatistics;
import com.miotech.kun.metadata.databuilder.model.DataSource;

import java.util.List;

public interface FieldStatisticsExtractor {

    List<FieldStatistics> extractFieldStatistics(Dataset dataset, DataSource dataSource);

}
