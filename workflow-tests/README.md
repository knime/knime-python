# Workflow Tests

The workflow tests are integration tests. They will run on all supported OSes
with different environments. Instead of testing many different combinations of
Python packages we test a few combinations. To test backward compatibility we
keep environments with old versions of packages. On `releases/` branches we run
the workflow tests with all environment definitions. On the master branch, we
only run the workflow test with the latest environment definition.
