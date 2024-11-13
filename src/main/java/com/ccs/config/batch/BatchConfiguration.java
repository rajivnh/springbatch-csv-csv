package com.ccs.config.batch;

import java.io.IOException;
import java.io.Writer;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.validation.BindException;

import com.ccs.model.EmpUdbData;

@EnableBatchProcessing
@Configuration
public class BatchConfiguration {
	@Autowired
	StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	JobBuilderFactory jobBuilderFactory;

	@Bean 
	public FlatFileItemReader<EmpUdbData> reader() {
		return new FlatFileItemReaderBuilder<EmpUdbData>()
	    .name("csvBuilder")
		.delimited()
        .delimiter(",")
        .names(new String[] {"empNumber", "empName", "empJoinDt", "empAddr"})
        .linesToSkip(1)
        .resource(new FileSystemResource("C:/rajiv/workspace-insursa/springbatch-csv-csv/src/main/resources/csv/employee_lite_in.csv"))
        .fieldSetMapper(new FieldSetMapper<EmpUdbData>() {
			@Override
			public EmpUdbData mapFieldSet(FieldSet fieldSet) throws BindException {
				return new EmpUdbData(fieldSet.readInt("empNumber"),
						fieldSet.readString("empName"),
						fieldSet.readString("empAddr"));
			}
        })
        .build();
	}
	
	@Bean
	public FlatFileItemWriter<EmpUdbData> writer() {
		FlatFileItemWriter<EmpUdbData> writer = new FlatFileItemWriter<EmpUdbData>();
		
		writer.setResource(new FileSystemResource("C:/rajiv/workspace-insursa/springbatch-csv-csv/src/main/resources/csv/employee_lite_out.csv"));
		
		writer.setHeaderCallback(new FlatFileHeaderCallback() {
			public void writeHeader(Writer writer) throws IOException {
                writer.write("EMP_NUMBER,EMP_NAME,EMP_ADDR");
            }
		});
		
		DelimitedLineAggregator<EmpUdbData> aggregator = new DelimitedLineAggregator<EmpUdbData>();

		BeanWrapperFieldExtractor<EmpUdbData> fieldExtractor = new BeanWrapperFieldExtractor<EmpUdbData>();
		fieldExtractor.setNames(new String[] {"empNumber", "empName", "empAddr"});
	
		aggregator.setFieldExtractor(fieldExtractor);
		writer.setLineAggregator(aggregator);

		return writer;
	}
	
	@Bean
	public Step executeStep() {
		return stepBuilderFactory.get("executeStep")
				.<EmpUdbData, EmpUdbData>chunk(200)
				.reader(reader())
				.writer(writer())
				.build();
	}
	
	@Bean
	public Job processJob() {
		return jobBuilderFactory.get("processJob")
				.incrementer(new RunIdIncrementer())
				.flow(executeStep())
				.end()
				.build();
	}
}
