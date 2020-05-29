package com.miotech.kun.workflow.common.operator.dao;

import com.miotech.kun.commons.testing.DatabaseTestBase;
import com.miotech.kun.workflow.common.operator.filter.OperatorSearchFilter;
import com.miotech.kun.workflow.core.model.common.Param;
import com.miotech.kun.workflow.core.model.operator.Operator;
import com.miotech.kun.workflow.testing.factory.MockOperatorFactory;
import com.miotech.kun.workflow.utils.WorkflowIdGenerator;
import org.junit.Test;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class OperatorDaoTest extends DatabaseTestBase {
    @Inject
    OperatorDao operatorDao;

    private void insertSampleData() {
        List<Operator> operators = MockOperatorFactory.createOperators(5);

        // Add "example" as prefix for 3 operators
        for (int i = 2; i < 5; i += 1) {
            Operator op = operators.get(i);
            operators.set(i, op.cloneBuilder().withName("example" + op.getName()).build());
        }

        operators.forEach(operator -> {
            operatorDao.create(operator);
        });
    }

    @Test
    public void create_newOperator_shouldPersist() {
        // Prepare
        List<Param> exampleParams = new ArrayList<>();
        exampleParams.add(Param.newBuilder().withName("x").withDescription("first param").build());
        exampleParams.add(Param.newBuilder().withName("y").withDescription("second param").build());
        Long id = WorkflowIdGenerator.nextOperatorId();
        Operator exampleOperator = Operator.newBuilder()
                .withId(id)
                .withName("foo")
                .withDescription("foo_description")
                .withParams(exampleParams)
                .withClassName("com.miotech.kun.foo.BashOperator")
                .withPackagePath("s3://storage.miotech.com/foo.jar")
                .build();

        // Process
        operatorDao.create(exampleOperator);

        // Validate
        Optional<Operator> fetchResult = operatorDao.getById(id);
        assertThat(fetchResult.isPresent(), is(true));
        Operator result = fetchResult.get();
        assertThat(result, samePropertyValuesAs(exampleOperator));
    }

    @Test(expected = RuntimeException.class)
    public void create_operatorsWithDuplicatedId_ExceptionThrown() {
        // Prepare
        Long id = WorkflowIdGenerator.nextOperatorId();
        Operator operator = Operator.newBuilder()
                .withId(id)
                .withName("foo")
                .withDescription("foo_description")
                .withParams(new ArrayList<>())
                .withClassName("com.miotech.kun.foo.BashOperator")
                .withPackagePath("s3://storage.miotech.com/foo.jar")
                .build();
        Operator duplicatedOperator = operator.cloneBuilder().build();
        assertThat(operator, samePropertyValuesAs(duplicatedOperator));

        // Process
        operatorDao.create(operator);
        operatorDao.create(duplicatedOperator);

        // should thrown exception
    }

    @Test
    public void search_withUnlimitedPageSize_shouldReturnFullResult() {
        // Prepare
        insertSampleData();
        OperatorSearchFilter defaultFilter = OperatorSearchFilter.newBuilder()
                .withPageNum(1)
                .withPageSize(Integer.MAX_VALUE)
                .build();

        // Process
        List<Operator> fullResults = operatorDao.search(defaultFilter);

        // Validate
        assertEquals(5, fullResults.size());
    }

    @Test
    public void search_withPagination_shouldLimitPageSize() {
        // Prepare
        // Insert 5 rows in total
        insertSampleData();

        OperatorSearchFilter filterPageOne = OperatorSearchFilter.newBuilder()
                .withPageNum(1)
                .withPageSize(3)
                .build();
        OperatorSearchFilter filterPageTwo = OperatorSearchFilter.newBuilder()
                .withPageNum(2)
                .withPageSize(3)
                .build();

        // Process
        List<Operator> firstPageResults = operatorDao.search(filterPageOne);
        List<Operator> secondPageResults = operatorDao.search(filterPageTwo);

        // Validate
        assertEquals(3, firstPageResults.size());
        assertEquals(2, secondPageResults.size());
    }

    @Test
    public void search_withInvalidPagination_shouldThrowException() {
        // Prepare
        insertSampleData();
        OperatorSearchFilter filterWithInvalidPageNum = OperatorSearchFilter.newBuilder()
                .withPageNum(-1)
                .withPageSize(5)
                .build();
        OperatorSearchFilter filterWithInvalidPageSize = OperatorSearchFilter.newBuilder()
                .withPageNum(1)
                .withPageSize(0)
                .build();

        // Process
        try {
            operatorDao.search(filterWithInvalidPageNum);
            fail();
        } catch (Exception e) {
            // Validate
            assertThat(e, instanceOf(IllegalArgumentException.class));
        }

        try {
            operatorDao.search(filterWithInvalidPageSize);
            fail();
        } catch (Exception e) {
            // Validate
            assertThat(e, instanceOf(IllegalArgumentException.class));
        }
    }

    @Test
    public void search_withKeyword_shouldWork() {
        // Prepare
        // 5 rows, 3 rows with name including substring "example"
        insertSampleData();
        OperatorSearchFilter filter = OperatorSearchFilter.newBuilder()
                .withKeyword("example")
                .withPageNum(1)
                .withPageSize(5)
                .build();

        // Process
        List<Operator> results = operatorDao.search(filter);

        // Validate
        assertEquals(3, results.size());
    }

    @Test
    public void search_withUnmatchedKeyword_shouldReturnEmptySet() {
        // Prepare
        // 5 rows, 3 rows with name including substring "example"
        insertSampleData();
        OperatorSearchFilter filter = OperatorSearchFilter.newBuilder()
                .withKeyword("exampleABC")
                .withPageNum(1)
                .withPageSize(5)
                .build();

        // Process
        List<Operator> results = operatorDao.search(filter);

        // Validate
        assertEquals(0, results.size());
    }

    @Test
    public void delete_WithProperId_shouldSuccess() {
        // Prepare
        insertSampleData();
        Optional<Operator> firstOperator = operatorDao.getById(1L);
        assertTrue(firstOperator.isPresent());

        // Process
        boolean rowAffected = operatorDao.deleteById(1L);

        // Validate
        assertTrue(rowAffected);
        Optional<Operator> firstOperatorRemoved = operatorDao.getById(1L);
        assertThat(firstOperatorRemoved.isPresent(), is(false));
    }

    @Test
    public void delete_WithNonExistId_shouldAffectNoRow() {
        // Prepare
        insertSampleData();
        Optional<Operator> firstOperator = operatorDao.getById(1L);
        assertTrue(firstOperator.isPresent());

        // Process
        boolean rowAffected = operatorDao.deleteById(999L);
        assertFalse(rowAffected);
    }

    @Test
    public void update_WithProperId_shouldSuccess() {
        // Prepare
        insertSampleData();

        Optional<Operator> firstOperator = operatorDao.getById(1L);

        // Process
        Operator updatedOperator = firstOperator.get().cloneBuilder()
                .withName("fooUpdated")
                .withPackagePath("s3://storage.miotech.com/fooUpdated.jar")
                .build();
        boolean rowAffected = operatorDao.updateById(1L, updatedOperator);

        // Validate
        assertTrue(rowAffected);

        Optional<Operator> updatedFirstOperatorOptional = operatorDao.getById(1L);
        assertTrue(updatedFirstOperatorOptional.isPresent());

        Operator updatedFirstOperator = updatedFirstOperatorOptional.get();
        assertThat(updatedFirstOperator, samePropertyValuesAs(updatedOperator));
    }
}
