package Amazon::DynamoDB::20120810;

use strict;
use warnings;

=head1 NAME

Amazon::DynamoDB::20120810 - interact with DynamoDB using API version 20120810

=head1 DESCRIPTION

=cut

use Future;
use Future::Utils qw(repeat try_repeat);
use POSIX qw(strftime);
use JSON::XS;
use MIME::Base64;
use List::Util;
use List::MoreUtils;
use B qw(svref_2object);
use HTTP::Request;

use Amazon::DynamoDB::SignatureV4;

my $json = JSON::XS->new;

=head2 new

Instantiates the API object.

Expects the following named parameters:

=over 4

=item * implementation - the object which provides a Future-returning C<request> method,
see L<Amazon::DynamoDB::NaHTTP> for example.

=item * host - the host (IP or hostname) to communicate with

=item * port - the port to use for HTTP(S) requests

=item * ssl - true for HTTPS, false for HTTP

=item * algorithm - which signing algorithm to use, default AWS4-HMAC-SHA256

=item * scope - the scope for requests, typically C<region/host/aws4_request>

=item * access_key - the access key for signing requests

=item * secret_key - the secret key for signing requests

=item * debug_failures - print errors if they occur

=item * max_retries - maximum number of retries for a request

=back

=cut

sub new {
    my $class = shift;
    bless { @_ }, $class
}

sub implementation { shift->{implementation} }
sub host { shift->{host} }
sub port { shift->{port} }
sub ssl { shift->{ssl} }
sub algorithm { 'AWS4-HMAC-SHA256' }
sub scope { shift->{scope} }
sub access_key { shift->{access_key} }
sub secret_key { shift->{secret_key} }
sub debug_failures { shift->{debug} }

sub max_retries { shift->{max_retries} }




=head2 create_table

Creates a new table. It may take some time before the table is marked
as active - use L</wait_for_table_status> to poll until the status changes.

Amazon Documentation:

L<http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html>

  $ddb->create_table(
     TableName => $table_name,
     ReadCapacityUnits => 2,
     WriteCapacityUnits => 2,
     AttributeDefinitions => {
         user_id => 'N',
         date => 'N',
     },
     KeySchema => ['user_id', 'date'],
     LocalSecondaryIndexes => [
         {
             IndexName => 'UserDateIndex',
             KeySchema => ['user_id', 'date'],
             Projection => {
                 ProjectionType => 'KEYS_ONLY',
             },
             ProvisionedThroughput => {
                 ReadCapacityUnits => 2,
                 WriteCapacityUnits => 2,
             }
         }
     ]
  );

=back

=cut

sub create_table {
    my $self = shift;
    my %args = @_;

    $args{ReadCapacityUnits} //= 2;
    $args{WriteCapacityUnits} //= 2;

    my %payload = (
        TableName => $args{TableName},
        ProvisionedThroughput => {
            ReadCapacityUnits => $args{ReadCapacityUnits},
            WriteCapacityUnits => $args{WriteCapacityUnits},
        }
    );

    if (defined($args{AttributeDefinitions})) {
        ref($args{AttributeDefinitions}) eq 'HASH' || Carp::confess("AttributeDefinitions should be a hash, each field is unique");
        foreach my $field_name (keys %{$args{AttributeDefinitions}}) {
            my $type = $args{AttributeDefinitions}->{$field_name};

            if (defined($type)) {
                $type =~ /^(S|N|B)$/ || Carp::confess("Invalid type specified for attribute '$field_name', must be S, N or B was $type");
            }
            push @{$payload{AttributeDefinitions}}, {
                AttributeName => $field_name,
                AttributeType => $type // 'S',
            }
        }
    }
    
    defined($args{KeySchema}) || Carp::confess("No KeySchema specified");
    ref($args{KeySchema}) eq 'ARRAY' || Carp::confess("KeySchema is not an array");
    scalar(@{$args{KeySchema}}) > 0 || Carp::confess("KeySchema requires at least one value");
    scalar(@{$args{KeySchema}}) <= 2 || Carp::confess("KeySchema can have at most two values");

    $payload{KeySchema} = _create_key_schema($args{KeySchema}, $args{AttributeDefinitions});

    foreach my $index_type ('GlobalSecondaryIndexes', 'LocalSecondaryIndexes') {
        
        if (defined($args{$index_type})) {
            ref($args{$index_type}) eq 'ARRAY' || Carp::confess("global_secondary_indexes is not an array");
            scalar(@{$args{$index_type}}) <= 5 || Carp::confess("Too many global secondary indexes specified, must be less than or equal to 5");

            foreach my $i (@{$args{$index_type}}) {
                defined($i->{IndexName}) || Carp::confess("No name specified in $index_type: " . Data::Dumper->Dump([$i]));
                my $r = {
                    IndexName => $i->{IndexName},
                    (($index_type eq 'GlobalSecondaryIndexes') ? 
                         (ProvisionedThroughput => {
                             ReadCapacityUnits => $i->{ProvisionedThroughput}->{ReadCapacityUnits} // 1,
                             WriteCapacityUnits => $i->{ProvisionedThroughput}->{WriteCapacityUnits} // 1,
                         }) : ()),
                    KeySchema => _create_key_schema($i->{KeySchema}, $args{AttributeDefinitions}),
                };

                defined($i->{Projection}) || Carp::confess("No projection defined for index named $i->{IndexName}");

                my $type = $i->{Projection}->{ProjectionType};
                defined($type) || Carp::confess("Missing type for projection for index named $i->{IndexName}");
                $type =~ /^(KEYS_ONLY|INCLUDE|ALL)$/ || Carp::confess("Unknown projection type specified: $type for index named $i->{IndexName}");
                
                $r->{Projection}->{ProjectionType} = $type;
                
                if (defined($i->{Projection}->{NonKeyAttributes})) {
                    my $attrs = $i->{Projection}->{NonKeyAttributes};
                    defined($attrs) || Carp::confess("No non key attributes specified");
                    ref($attrs) eq 'ARRAY' || Carp::confess("NonKeyAttributes is not an array");
                    # Can't validate these attribute names since they aren't part of the key.
                    $r->{Projection}->{NonKeyAttributes} = $attrs;
                }
                push @{$payload{$index_type}}, $r;
            }
        }
    }

    my $req = $self->make_request(
        target => 'CreateTable',
        payload => \%payload,
    );
    $self->_process_request($req)
}

=head2 describe_table

Describes the given table.

Amazon Documentation:

L<http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeTable.html>

  $ddb->describe_table(TableName => $table_name);

=cut

sub describe_table {
    my $self = shift;
    my %args = @_;
    my $req = $self->make_request(
        target => 'DescribeTable',
        payload => _make_payload(\%args,
                                 'TableName'));
    $self->_process_request($req,
                            sub { 
                                my $content = shift; 
                                $json->decode($content)->{Table};
                            });
}

=head2 delete_table

Delete a table.

Amazon Documentation:

L<http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html>

  $ddb->delete_table(TableName => $table_name)

=cut

sub delete_table {
    my $self = shift;
    my %args = @_;

    my $req = $self->make_request(
        target => 'DeleteTable',
        payload => _make_payload(\%args,
                                 'TableName'));
    $self->_process_request($req,
                            sub {
                                my $content = shift;
                                $json->decode($content)->{TableDescription}
                            });
}

=head2 wait_for_table_status

Waits for the given table to be marked as active.

=over 4

=item * TableName - the table name

=item * WaitInterval - default wait interval in seconds.
 
=item * DesiredStatus - status to expect before completing.  Defaults to ACTIVE

=back

  $ddb->wait_for_table_status(TableName => $table_name);

=cut

sub wait_for_table_status {
    my $self = shift;
    my %args = @_;
    
    defined($args{TableName}) || Carp::confess("No TableName specified");
    repeat {
        my $retry = shift;
        
        $self->{implementation}->delay($retry ? ($args{WaitInterval} || 2) : 0)
            ->then(sub {
                       $self->describe_table(%args) 
                   });
    } until => sub {
        my $f = shift;
        my $status = $f->get->{TableStatus};
        $status eq ($args{DesiredStatus} // 'ACTIVE')
    };
}

=head2 each_table

Run code for all current tables.

Takes a coderef as the first parameter, will call this for each table found.

Amazon Documentation:

L<http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ListTables.html>


  my @all_tables;    
  $ddb->each_table(
        sub {
            my $table_name =shift;
            push @all_tables, $table_name;
        });

=cut

sub each_table {
    my $self = shift;
    my $code = shift;
    defined($code) || Carp::confess("No callback passed to call for each table");
    ref($code) eq 'CODE' || Carp::confess("Callback is not a code reference");
    my %args = @_;

    my $finished = 0;
    try_repeat {
        my $req = $self->make_request(
            target => 'ListTables',
            payload => _make_payload(\%args,
                                     'ExclusiveStartTableName',
                                     'Limit'));
        $self->_process_request($req,
                                sub {
                                    my $result = shift;
                                    my $data = $json->decode($result);
                                    for my $tbl (@{$data->{TableNames}}) {
                                        $code->($tbl);
                                    }
                                    $args{ExclusiveStartTableName} = $data->{LastEvaluatedTableName};
                                    if (!defined($args{ExclusiveStartTableName})) {
                                        $finished = 1 
                                    }
                                });
    } while => sub { !$finished };
}

=head2 put_item

Writes a single item to the table.

Amazon Documentation:

L<http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html>

  $ddb->put_item(
     TableName => $table_name,
     Item => {
       name => 'Test Name'
     },
     ReturnValues => 'ALL_OLD');

=cut

sub put_item {
    my $self = shift;
    my %args = @_;

    my $req = $self->make_request(
        target => 'PutItem',
        payload => _make_payload(\%args, 
                                 'ConditionalOperator',
                                 'Expected',
                                 'Item',
                                 'ReturnConsumedCapacity',
                                 'ReturnItemCollectionMetrics',
                                 'ReturnValues',
                                 'TableName'));
                             
    $self->_process_request($req, \&_decode_single_item_change_response);
}


=head2 update_item

Updates a single item in the table.

Amazon Documentation:

L<http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html>

  $ddb->update_item(
        TableName => $table_name,
        Key => {
            user_id => 2
        },
        AttributeUpdates => {
            name => {
                Action => 'PUT',
                Value => "Rusty Conover-3",
            },
            favorite_color => {
                Action => 'DELETE'
            },
            test_numbers => {
                Action => 'DELETE',
                Value => [500]
            },
            added_number => {
                Action => 'ADD',
                Value => 5,
            },
            subtracted_number => {
                Action => 'ADD',
                Value => -5,
            },
        });

=cut

sub update_item {
    my $self = shift;
    my %args = @_;

    my $req = $self->make_request(
        target => 'UpdateItem',
        payload => _make_payload(\%args, 
                                 'AttributeUpdates',
                                 'ConditionalOperator',
                                 'Expected',
                                 'Key',
                                 'ReturnConsumedCapacity',
                                 'ReturnItemCollectionMetrics',
                                 'ReturnValues',
                                 'TableName'));

    $self->_process_request($req, \&_decode_single_item_change_response);
}

=head2 delete_item

Deletes a single item from the table.

Amazon Documentation:

L<http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html>

  $ddb->delete_item(
    TableName => $table_name,
    Key => {
      user_id => 5
  });

=cut



sub delete_item {
    my $self = shift;
    my %args = @_;
    
    my $req = $self->make_request(
        target => 'DeleteItem',
        payload => _make_payload(\%args, 
                                 'ConditionalOperator',
                                 'Expected',
                                 'Key',
                                 'ReturnConsumedCapacity',
                                 'ReturnItemCollectionMetrics',
                                 'ReturnValues',
                                 'TableName'));
            
    $self->_process_request($req, \&_decode_single_item_change_response);
}



=head2 get_item

Retrieve an items from one tables.

Amazon Documentation:

L<http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html>

  my $found_item;
  my $get = $ddb->get_item(
    sub {
      $found_item = shift;
    },
    TableName => $table_name,
    Key => {
      user_id => 6
    });

=cut

sub get_item {
    my $self = shift;
    my $code = shift;
    my %args = @_;

    my $req = $self->make_request(
        target => 'GetItem',
        payload => _make_payload(\%args, 
                                 'AttributesToGet',
                                 'ConsistentRead',
                                 'Key',
                                 'ReturnConsumedCapacity',
                                 'TableName'));

    $self->_process_request(
        $req, 
        sub {
            my $result = shift;
            my $data = $json->decode($result);
            $code->(_decode_item_attributes($data->{Item}));
        });
}


=head2 batch_write_item

Put or delete a collection of items.  

Has no restriction on the number of items able to be processed at one time.

Amazon Documentation:

L<http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html>

  $ddb->batch_write_item(
    RequestItems => {
       books => [
            {
                DeleteRequest => {
                    book_id => 3000,
                }
            },
       ],
       users => [
            {
                PutRequest => {
                    user_id => 3000,
                    name => "Test batch write",
                }
            },
            {
                PutRequest => {
                    user_id => 3001,
                    name => "Test batch write",
                }
            }
        ]
    });

=cut


sub batch_write_item {
    my $self = shift;
    my %args = @_;

    my @all_requests;

    foreach my $table_name (keys %{$args{RequestItems}}) {
        # Item.
        my $table_items = $args{RequestItems}->{$table_name};
            
        my $seen_type;
        foreach my $item (@$table_items) {
            my $r;
            if (defined($item->{DeleteRequest}) && defined($item->{PutRequest})) {
                die("Cannot have DeleteRequest and PutRequest operations on the same table");
            }

            if (!(defined($item->{DeleteRequest}) || defined($item->{PutRequest}))) {
                die("Must have either a DeleteRequest or PutRequest: " . Data::Dumper->Dump([$item]));
            }

            foreach my $t (['DeleteRequest', 'Key'], ['PutRequest', 'Item']) {
                if (defined($item->{$t->[0]})) {
                    my $key = $item->{$t->[0]}->{$t->[1]};
                    defined($key) || Carp::confess("No $t->[1] defined for $t->[0]");
                    foreach my $k (keys %$key) {
                        # Don't bother encoding undefined values, same behavior as put_item
                        if (defined($key->{$k})) {
                            $r->{$t->[0]}->{$t->[1]}->{$k} = { _encode_type_and_value($key->{$k}) };
                        }
                    }
                }
            }
            if (defined($r)) {
                push @all_requests, [$table_name, $r];
            }
        }
    }

    try_repeat {
        my %payload = (
            ReturnConsumedCapacity => $args{ReturnConsumedCapacity} // 'NONE',
            ReturnItemCollectionMetrics => $args{ReturnItemCollectionMetrics} // 'NONE',
        );

        #            print "Pending requests: " . scalar(@all_requests) . "\n";
        my @records = splice @all_requests, 0, List::Util::min(25, scalar(@all_requests));
            

        foreach my $record (@records) {
            push @{$payload{RequestItems}->{$record->[0]}}, $record->[1];
        }
            

        my $req = $self->make_request(
            target => 'BatchWriteItem',
            payload => \%payload,
        );

        $self->_process_request(
            $req,
            sub {
                my $result = shift;
                my $data = $json->decode($result);
                    
                if (defined($data->{UnprocessedItems})) {
                    foreach my $table_name (keys %{$data->{UnprocessedItems}}) {
                        push @all_requests, map { [$table_name, $_] } @{$data->{UnprocessedItems}->{$table_name}};
                    }
                }
                return $data;
            })->on_fail(sub { 
                            @all_requests = ();
                        });
    } until => sub { scalar(@all_requests) == 0 };
}




=head2 batch_get_item

Retrieve a batch of items from one or more tables.

Takes a coderef which will be called for each found item.

Amazon Documentation:

L<http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html>

Additional Parameters:

=over

=item * ResultLimit - limit on the total number of results to return.

=back

  $ddb->batch_get_item(
    sub {
        my ($table, $item) = @_;
    },
    RequestItems => {
        $table_name => {
            ConsistentRead => 'true',
            AttributesToGet => ['user_id', 'name'],
            Keys => [
                {
                    user_id => 1,
                },
            ],
        }
    })

=cut

sub batch_get_item {
    my $self = shift;
    my $code = shift;
    my %args = @_;

    my @all_requests;
    my $table_flags = {};

    foreach my $table_name (keys %{$args{RequestItems}}) {
        my $table_details = $args{RequestItems}->{$table_name};

        # Store these flags for later.
        map { 
            if (defined($table_details->{$_})) {
                $table_flags->{$_} = $table_details->{$_};
            }
        } ('ConsistentRead', 'AttributesToGet');

        defined($table_details->{Keys}) || die("No defined keys to retrieve for table $table_name");
        ref($table_details->{Keys}) eq 'ARRAY' || Carp::confess('Keys must be an arrayref');
            
        foreach my $item (@{$table_details->{Keys}}) {
            my $r = {};
            foreach my $key_field (keys %$item) {
                $r->{$key_field} = { _encode_type_and_value($item->{$key_field}) };
            }
            push @all_requests, [$table_name, $r];
        }
    }

    my $records_seen =0;
    try_repeat {

        my %payload = (
            ReturnConsumedCapacity => $args{ReturnConsumedCapacity} // 'NONE',
        );

        # Only try 100 requests at one time.
        my @records = splice @all_requests, 0, List::Util::min(100, scalar(@all_requests));


        foreach my $record (@records) {
            push @{$payload{RequestItems}->{$record->[0]}->{Keys}}, $record->[1];
        }
            
        foreach my $seen_table_name (grep { defined($table_flags->{$_}) } List::MoreUtils::uniq(map { $_->[0] } @records)) {
            $payload{RequestItems}->{$seen_table_name} = {
                %{$table_flags->{$seen_table_name}},
                Keys => $payload{RequestItems}->{$seen_table_name}->{Keys}
            };
        }

        my $req = $self->make_request(
            target => 'BatchGetItem',
            payload => \%payload,
        );

        $self->_process_request(
            $req,
            sub {
                my $result = shift;
                my $data = $json->decode($result);
                foreach my $table_name (keys %{$data->{Responses}}) {
                    foreach my $item (@{$data->{Responses}->{$table_name}}) {
                        $code->($table_name, _decode_item_attributes($item));
                        $records_seen += 1;
                        if (defined($args{ResultLimit}) &&$records_seen >= $args{ResultLimit}) {
                            @all_requests = ();
                            return $data;
                        }
                    }
                }
                    
                if (defined($data->{UnprocessedKeys})) {
                    foreach my $table_name (keys %{$data->{UnprocessedKeys}}) {
                        push @all_requests, map { [$table_name, $_] } @{$data->{UnprocessedKeys}->{$table_name}->{Keys}};
                    }
                }
                return $data;
            })->on_fail(sub { 
                            @all_requests = ();
                        });
    } until => sub { scalar(@all_requests) == 0 };
}


sub query {
    my $self = shift;
    my $code = shift;
    
    # If the user is just asking for a count, don't require them to supply a callback.
    if (ref($code) ne 'CODE') {
        unshift @_, $code;
    }
    
    my %args = @_;

    my $payload = _make_payload(\%args,
                                'AttributesToGet',
                                'ConsistentRead',
                                'ConditionalOperator',
                                'ExclusiveStartKey',
                                'IndexName',
                                'Limit',
                                'QueryFilter',
                                'ReturnConsumedCapacity',
                                'ScanIndexForward',
                                'Select',
                                'TableName');
    

    defined($args{KeyConditions}) || Carp::confess("No KeyConditions specified for query, conditions are required.");
    ref($args{KeyConditions}) eq 'HASH' || Carp::confess("KeyConditions must be a hashref");

    foreach my $key_name (keys %{$args{KeyConditions}}) {
        my $key_details = $args{KeyConditions}->{$key_name};
        ref($key_details) eq 'HASH' || Carp::confess("KeyConditions for key $key_name are not a hashref");
        my $compare_op = $key_details->{ComparisonOperator} // 'EQ';
        $compare_op =~ /^(EQ|LE|LT|GE|GT|BEGINS_WITH|BETWEEN)$/
            || Carp::confess("Unknown comparison operator specified: $compare_op");
        
        $payload->{KeyConditions}->{$key_name} = {
            AttributeValueList => _encode_attribute_value_list($key_details->{AttributeValueList}, $compare_op),
            ComparisonOperator => $compare_op
        };
    }

    $self->_scan_or_query_process('Query', $payload, $code, \%args);
}




=head2 scan

Scan a table for values with an optional filter expression.

Amazon Documentation:

L<http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html>

Additional parameters:

=over 4

=item * ResultLimit - maximum number of items to return

=back

  $ddb->scan(
    sub {
      my $item = shift;
      push @found_items, $item;
    },
    TableName => $table_name,
    ScanFilter => {
      user_id => {
        ComparisonOperator => 'NOT_NULL',
      }
    });

=cut

sub scan {
    my $self = shift;
    my $code = shift;
        
    # If the user is just asking for a count, don't require them to supply a callback.
    if (ref($code) ne 'CODE') {
        unshift @_, $code;
    }

    my %args = @_;

    my $payload = _make_payload(\%args,
                                'AttributesToGet',
                                'ExclusiveStartKey',
                                'Limit',
                                'ReturnConsumedCapacity',
                                'ScanFilter',
                                'Segment',
                                'Select',
                                'TableName',
                                'TotalSegments',
                            );

    $self->_scan_or_query_process('Scan', $payload, $code, \%args);
}

=head1 METHODS - Internal

The following methods are intended for internal use and are documented
purely for completeness - for normal operations see L</METHODS> instead.

=head2 make_request

Generates an L<HTTP::Request>.

=cut

sub make_request {
    my $self = shift;
    my %args = @_;
    my $api_version = '20120810';
    my $host = $self->host;
    my $target = $args{target};
    my $js = JSON::XS->new;
    my $req = HTTP::Request->new(
        POST => (($self->ssl) ? 'https' : 'http') . '://' . $self->host . ($self->port ? (':' . $self->port) : '') . '/'
    );
    $req->header( host => $host );
    # Amazon requires ISO-8601 basic format
    my $now = time;
    my $http_date = strftime('%Y%m%dT%H%M%SZ', gmtime($now));
    my $date = strftime('%Y%m%d', gmtime($now));
    $req->protocol('HTTP/1.1');
    $req->header( 'Date' => $http_date );
    $req->header( 'x-amz-target', 'DynamoDB_'. $api_version. '.'. $target );
    $req->header( 'content-type' => 'application/x-amz-json-1.0' );
    my $payload = $js->encode($args{payload});
    $req->content($payload);
    $req->header( 'Content-Length' => length($payload));
    my $amz = Amazon::DynamoDB::SignatureV4->new(
        version    => 4,
        algorithm  => $self->algorithm,
        access_key => $self->access_key,
        scope      => $date . "/" . $self->scope,
        secret_key => $self->secret_key,
    );
    $amz->from_http_request($req);
    $req->header(Authorization => $amz->calculate_signature);
    $req
}

sub _request {
    my $self = shift;
    my $req = shift;
    $self->implementation->request($req)
}


# Since scan and query have the same type of responses share the processing.
sub _scan_or_query_process {
    my ($self, $target, $payload, $code, $args) = @_;

    my $finished = 0;
    my $records_seen = 0;
    my $repeat = try_repeat {
        
        my $req = $self->make_request(
            target => $target,
            payload => $payload,
        );
        
        $self->_process_request(
            $req,
            sub {
                my $result = shift;
                my $data = $json->decode($result);
                
                for my $entry (@{$data->{Items}}) {
                    $code->(_decode_item_attributes($entry));
                    $records_seen += 1;
                    if (defined($args->{ResultLimit}) && $records_seen >= $args->{ResultLimit}) {
                        $finished = 1;
                        last;
                    }
                }
                $payload->{ExclusiveStartKey} = $data->{LastEvaluatedKey};
                
                if (!defined($payload->{ExclusiveStartKey})) {
                    $finished = 1;
                }
                return $data;
            })
            ->on_fail(sub {
                          $finished = 1;
                      });
    } until => sub { $finished };
}


=head1 FUNCTIONS - Internal

=head2 _encode_type_and_value

Returns an appropriate type (N, S, SS etc.) and stringified/encoded value for the given
value.

DynamoDB only uses strings even if there is a Numeric value specified,
so while the type will be expressed as a Number the value will be
stringified.

C<http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataFormat.html>

=cut

sub _encode_type_and_value {
    my $v = shift;
    my $type;

    if (ref($v)) {
        # An array maps to a sequence
        if (ref($v) eq 'ARRAY') {
            # Any refs mean we're sending binary data
            
            # Start by guessing we have an array of numeric strings,
            # but on the first value we encoutner that is either a reference
            # or a variable that isn't an integer or numeric.  Stop.
            $type = 'NS';
            foreach my $value (@$v) {
                if (ref($value)) {
                    $type = 'BS';
                    last;
                }
                my $element_flags = B::svref_2object(\$value)->FLAGS;
                if ($element_flags & (B::SVp_IOK | B::SVp_NOK)) {
                    next;
                }
                $type = 'SS';
                last;
            }
        } else {
            ref($v) eq 'SCALAR' || Carp::confess("Reference found but not a scalar");
            $type = 'B';
        }
    } else {
        my $flags = B::svref_2object(\$v)->FLAGS;
        if ($flags & B::SVp_POK) {
            $type = 'S';
        } elsif ($flags & (B::SVp_IOK | B::SVp_NOK)) {
            $type = 'N';
        } else {
            $type = 'S';
        }
    }
    
    if ($type eq 'N' || $type eq 'S') {
        defined($v) || Carp::confess("Attempt to encode undefined value");
        return ($type, "$v");
    } elsif ($type eq 'B') {
        return ($type, MIME::Base64::encode_base64(${$v}, ''));
    } elsif ($type eq 'NS' || $type eq 'SS') {
        return ($type, [map { "$_" } @$v]);
    } elsif ($type eq 'BS') {
        return ($type, [map { MIME::Base64::encode_base64(${$_}, '') } @$v]);
    } else {
        die("Unknown type for quoting and escaping: $type");
    }
}

sub _decode_type_and_value {
    my ($type, $value) = @_;

    if ($type eq 'S' || $type eq 'SS') {
        return $value;
    } elsif ($type eq 'N') {
        return  0+$value;
    } elsif ($type eq 'B') {
        return \MIME::Base64::decode_base64($value);
    } elsif ($type eq 'BS') {
        return [map { \MIME::Base64::decode_base64($_) } @$value];
    } elsif ($type eq 'NS') {
        return [map { 0+$_} @$value];
    } else {
        die("Don't know how to decode type: $type");
    }
}


sub _decode_item_attributes {
    my $item = shift;
    my $r;
    foreach my $key (keys %$item) {
        my $type = (keys %{$item->{$key}})[0];
        my $value = $item->{$key}->{$type};
        $r->{$key} = _decode_type_and_value($type, $item->{$key}->{$type});
    }
    return $r;
}

sub _process_request {
    my ($self, $req, $done) = @_;
    my $current_retry = 0;
    my $do_retry = 1;
    try_repeat {
        $do_retry = 0;
        
        my $sleep_amount = 0;
        if ($current_retry > 0) {
            $sleep_amount = (2 ** $current_retry * 50)/1000;
        }

        my $complete = sub {
            $self->_request($req)->transform(
                fail => sub {
                    my ($status, $resp, $req)= @_;
                    my $r;
                    if (defined($resp)) {
                        if ($resp->code == 500) {
                            $do_retry = 1;
                            $current_retry++;
                        } elsif ($resp->code == 400) {
                            $r = $json->decode($resp->decoded_content);
                            if ($r->{__type} =~ /ProvisionedThroughputExceededException$/) {
                                # Need to sleep
                                $do_retry = 1;
                                $current_retry++;
                                    
                                if (defined($self->max_retries()) && $current_retry > $self->max_retries()) {
                                    $do_retry = 0;
                                }
                                
                            } else {
                                # extract the type into a better prettyier name.
                                if ($r->{__type} =~ /^com\.amazonaws\.dynamodb\.v20120810#(.+)$/) {
                                    $r->{type} = $1;
                                }
                            }
                        }
                    }

                    if (!$do_retry) {
                        if ($self->debug_failures()) {
                            print "DynamoDB Failure: $status\n";
                            if (defined($resp)) {
                                print "response:\n";
                                print $resp->as_string() . "\n";
                            }
                            if (defined($req)) {
                                print "Request:\n";
                                print $req->as_string() . "\n";
                            }
                        }
                        return $r || $status;
                    }
                },
                done => $done);
        };

        if ($sleep_amount > 0) {
            $self->{implementation}->delay($sleep_amount)->then($complete);
        } else {
            $complete->();
        }
    } until => sub { !$do_retry };
}

my $encode_key = sub {
    my $source = shift;
    my $r;
    foreach my $k (keys %$source) {
        my $v = $source->{$k};	
        # There is no sense in encoding undefined values or values that 
        # are the empty string.
        if (defined($v) && $v ne '') {
            # Reference $source->{$k} since the earlier test may cause
            # the value to be stringified.
            $r->{$k} = { _encode_type_and_value($source->{$k}) };
        }
    }
    return $r;
};


sub _encode_attribute_value_list {
    my $value_list = shift;
    my $compare_op = shift;

    if ($compare_op =~ /^(EQ|NE|LE|LT|GE|GT|CONTAINS|NOT_CONTAINS|BEGINS_WITH)$/) {
        defined($value_list) || Carp::confess("No defined value for comparison operator: $compare_op");
        $value_list = [ { _encode_type_and_value($value_list) } ];
    } elsif ($compare_op eq 'IN') {
        if (!ref($value_list)) {
            $value_list = [$value_list];
        }
        $value_list = [ map { { _encode_type_and_value($_) } } @$value_list];
    } elsif ($compare_op eq 'BETWEEN') {
        ref($value_list) eq 'ARRAY' || Carp::confess("Use of BETWEEN comparision operator requires an array");
        scalar(@$value_list) == 2 || Carp::confess("BETWEEN comparison operator requires two values");
        $value_list = [ map { { _encode_type_and_value($_) } } @$value_list];
    }
    return $value_list;
}

my $encode_filter = sub {
    my $source = shift;

    my $r;

    foreach my $field_name (keys %$source) {
        my $f = $source->{$field_name};
        my $compare_op = $f->{ComparisonOperator} // 'EQ';
        $compare_op =~ /^(EQ|NE|LE|LT|GE|GT|NOT_NULL|NULL|CONTAINS|NOT_CONTAINS|BEGINS_WITH|IN|BETWEEN)$/ 
            || Carp::confess("Unknown comparison operator specified: $compare_op");
        
        $r->{$field_name} = {
            ComparisonOperator => $compare_op,
            (defined($f->{AttributeValueList}) ? (AttributeValueList => _encode_attribute_value_list($f->{AttributeValueList}, $compare_op)) : ())
        };
    }
    return $r;
};

my $parameter_type_definitions = {
    AttributesToGet => {
        source_type => 'ARRAY',
    },
    AttributeUpdates => {
        encode => sub {
            my $source = shift;
            my $r;
            foreach my $k (keys %$source) {
                my $op = $source->{$k};
                $r->{$k} = {
                    (defined($op->{Action}) ? (Action => $op->{Action}) : ()),
                    (defined($op->{Value}) ? (Value => { _encode_type_and_value($op->{Value}) }) : ()),
                };
            }
            return $r;
        }
    },
    # should be a boolean
    ConsistentRead => {},
    ConditionalOperator => {
        allowed_values => ['AND', 'OR'],
    },
    ExclusiveStartKey => {
        source_type => 'HASH',
        encode => $encode_key,
    },
    ExclusiveStartTableName => {},    
    Expected => {
        source_type => 'HASH',
        encode => sub {
            my $source = shift;
            my $r;
            foreach my $key (keys %$source) {
                my $info = $source->{$key};

                if (defined($info->{AttributeValueList}) ) {
                    $r->{$key}->{AttributeValueList} = _encode_attribute_value_list($info->{AttributeValueList}, $info->{ComparisonOperator});
                }

                if (defined($info->{Exists})) {
                    $r->{$key}->{Exists} = $info->{Exists};
                }

                if (defined($info->{ComparisonOperator})) {
                    $r->{$key}->{ComparisonOperator} = $info->{ComparisonOperator};
                }
                
                if (defined($info->{Value})) {
                    $r->{$key}->{Value} = { _encode_type_and_value($info->{Value}) };
                }
            }
            return $r;
        },
    },
    IndexName => {},
    Item => {
        source_type => 'HASH',
        encode => $encode_key,
        required => 1,
    },
    Key => {
        source_type => 'HASH',
        encode => $encode_key,
        required => 1,
    },
    Limit => {
        type_check => 'integer',
    },
    QueryFilter => {
        source_type => 'HASH',
        encode => $encode_filter,
    },
    ReturnConsumedCapacity => {
        allowed_values => ['INDEXES', 'TOTAL', 'NONE'],
    },
    ReturnItemCollectionMetrics => {
        allowed_values => ['NONE', 'SIZE'],
    },
    ReturnValues => {
        allowed_values => ['NONE', 'ALL_OLD', 'UPDATED_OLD', 'ALL_NEW', 'UPDATED_NEW'],
    },
    ScanIndexForward => {},
    ScanFilter => {
        source_type => 'HASH',
        encode => $encode_filter,
    },
    Segment => {
        type_check => 'integer',
    },
    Select => {
        allowed_values => ['ALL_ATTRIBUTES', 'ALL_PROJECTED_ATTRIBUTES', 'SPECIFIC_ATTRIBUTES', 'COUNT'],
    },
    TableName => {
        required => 1
    },
    TotalSegments => {
        type_check => 'integer',
    },
};




# Build a parameter hash from all of the standardized parameters.
sub _make_payload {
    my $args = shift;
    my @field_names = @_;

    my %r;
    foreach my $field_name (@field_names) {
        my $value = $args->{$field_name};
        my $def = $parameter_type_definitions->{$field_name} || Carp::confess("Unknown parameter type: $field_name");
        if ($def->{required} && !defined($value)) {
            Carp::confess("Parameter $field_name is not defined and it is required");
        }
        if (defined($value)) {
            if ($def->{allowed_values} && scalar(grep { $_ eq $value } @{$def->{allowed_values}}) == 0) {
                Carp::confess("$field_name is specified to be '$value' but it is not an allowed value. Valid values are: " . join(",", @{$def->{allowed_values}}));
            }
            
            if ($def->{source_type} && ref($value) ne $def->{source_type}) {
                Carp::confess("$field_name is specified to be of type $def->{source_type} but it is of type: " . ref($value));
            }

            if ($def->{type_check} && $def->{type_check} eq 'integer') {
                $value =~ /^\d+$/ || Carp::confess("$field_name is specified to be an integer but the value is not an integer: $value");
                $value = int($value);
            }

        } else {
            if ($def->{defined_default}) {
                $value = $def->{defined_default};
            }
        }

        if (defined($def->{encode})) {
            $value = $def->{encode}->($value);
        }

        if (defined($value)) {
            $r{$field_name} = $value;
        }
    }
    return \%r;
}

sub _decode_single_item_change_response {
    my $r = $json->decode(shift);
    if (defined($r->{Attributes})) {
        $r->{Attributes} = _decode_item_attributes($r->{Attributes});
    }
    
    if (defined($r->{ItemCollectionMetrics})) {
        foreach my $key (keys %{$r->{ItemCollectionMetrics}}) {
            foreach my $key_part (keys %{$r->{ItemCollectionMetrics}->{$key}}) {
                $r->{ItemCollectionMetrics}->{$key}->{$key_part} = _decode_item_attributes($r->{ItemCollectionMetrics}->{$key})
            }
        }
    }    
    return $r;
}


sub _create_key_schema {
    my ($source, $known_fields) = @_;
    defined($source) || die("No source passed to create_key_schema");
    defined($known_fields) || die("No known fields passed to create_key_schmea");
    my @r;
    foreach my $field_name (@$source) {
        defined($known_fields->{$field_name}) || Carp::confess("Unknown field specified '$field_name' in schema, must be defined in fields.  schema:" . Data::Dumper->Dump([$source]));
        push @r, {
            AttributeName => $field_name,
            KeyType       => (scalar(@r) ? 'RANGE' : 'HASH')
        };
    }
    return \@r;
};



1;

__END__

