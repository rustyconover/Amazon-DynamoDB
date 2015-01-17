#!perl
use strict;
use warnings;
use lib ('lib', './t');
use Test::Most;
use TestSettings;

unless ( $ENV{'AMAZON_DYNAMODB_LOCAL_TESTS'} ) {
    plan skip_all => 'You need to be running DyanamoDB local for this test, then explictly enable it with $ENV{AMAZON_DYNAMODB_LOCAL_TESTS}';
} else {
    plan tests => 7;
}

bail_on_fail;

my $ddb = TestSettings::get_ddb(
    host => 'localhost',
    port => 8000,
    ssl  => 0,
);

my $table_name = TestSettings::random_table_name();

my $create = $ddb->create_table(TableName => $table_name,
                                ReadCapacityUnits => 2,
                                WriteCapacityUnits => 2,
                                AttributeDefinitions => {
                                    user_id => 'N',
                                },
                                KeySchema => ['user_id'],
                            );

ok($create->is_done, "Create request was completed");

my $wait = $ddb->wait_for_table_status(TableName => $table_name);

ok($wait->is_done, "Created table is ready");

my $source_data = {
    user_id => 1,
    test_string => 'foobar',
    test_undef => undef,
    test_empty => '',
    test_arrayref => [
        'A string',
        1,
        undef,
        '',
        {
            foo    => 'bar',
            bar    => 5,
            baz    => undef,
            foobar => '',
        },
    ],
    test_hashref => {
        foo    => 'bar',
        bar    => 5,
        baz    => undef,
        foobar => '',
        test_arrayref => [
            'A string',
            1,
            undef,
            '',
        ],
    },
};

my $put;

lives_ok
    {
        $put = $ddb->put_item(TableName => $table_name,
                             Item => $source_data);
    }
    q|put_item doesn't throw exception|,
;

ok($put->is_done, "put_item completed successfully");

my $found_item;
my $get = $ddb->get_item(
    sub {
        $found_item = shift;
    },
    TableName => $table_name,
    Key => {
        user_id => $source_data->{user_id}
    });

ok($get->is_done, "get_item completed ok");

cmp_deeply(
    $found_item,
    {%$source_data},
    'retrieved data matches input data',
);

ok($ddb->delete_table(TableName => $table_name)->is_done, "Successfully deleted table named $table_name");
