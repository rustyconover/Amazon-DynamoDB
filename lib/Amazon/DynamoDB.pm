package Amazon::DynamoDB;

# ABSTRACT: Abstract API support for Amazon DynamoDB
use strict;
use warnings;

our $VERSION = '0.002';

=head1 NAME

Amazon::DynamoDB - support for the AWS DynamoDB API

=head1 SYNOPSIS

  my $ddb = Amazon::DynamoDB->new(
     implementation => 'Amazon::DynamoDB::LWP',
     version        => '20120810',
     access_key     => 'access_key',
     secret_key     => 'secret_key',
     host => 'dynamodb.us-east-1.amazonaws.com',
     scope => 'us-east-1/dynamodb/aws4_request',
     ssl => 1,
     debug => 1);

  $ddb->batch_get_item(
     sub {
       my $tbl = shift;
       my $data = shift;
       print "Batch get: $tbl had " . join(',', %$data) . "\n";
     },
     RequestItems => {
       $table_name => { 
         Keys => [
           { 
             name => 'some test name here',
           }
         ],
         AttributesToGet => [qw(name age)],
       }
      })->get;

=head1 DESCRIPTION

Provides a L<Future>-based API for Amazon's DynamoDB REST API.
See L<Amazon::DynamoDB::20120810> for available methods.

Current implementations for issuing the HTTP requests:

=over 4

=item * L<Amazon::DynamoDB::NaHTTP> - use L<Net::Async::HTTP>
for applications based on L<IO::Async> (this gives nonblocking behaviour)

=item * L<Amazon::DynamoDB::LWP> - use L<LWP::UserAgent> (will
block, timeouts are unlikely to work)

=item * L<Amazon::DynamoDB::MojoUA> - use L<Mojo::UserAgent>,
should be suitable for integration into a L<Mojolicious> application.



=back

=cut

use Amazon::DynamoDB::20120810;
use Module::Load;

=head1 METHODS

=cut

sub new {
    my $class = shift;
    my %args = @_;
    $args{implementation} //= __PACKAGE__ . '::LWP';
    unless (ref $args{implementation}) {
        Module::Load::load($args{implementation});
        $args{implementation} = $args{implementation}->new(%args);
    }
    my $version = delete $args{version} || '201208010';
    my $pkg = __PACKAGE__ . '::' . $version;
    if (my $code = $pkg->can('new')) {
        $class = $pkg if $class eq __PACKAGE__;
        return $code->($class, %args)
    }
    die "No support for version $version";
}

1;

__END__

=head1 SEE ALSO

=over 4

=item * L<Net::Amazon::DynamoDB> - supports the older (2011) API with v2 signing, so it doesn't work with L<DynamoDB Local|http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.html>.

=item * L<AWS::CLIWrapper> - alternative approach using wrappers around AWS commandline tools

=item * L<Amazon::DynamoDB> - the parent of this module.

=back


=head1 IMPLEMENTATION PHILOSOPHY

Attempts to stick as close to Amazon's API as possible while making some inconvenient limits
easy to work around.

Parameters are named the same, return values are as described.   Documentation for each method
is commonly found at:

L<http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Operations.html>

=back


=head1 AUTHOR

Rusty Conover <rusty@luckydinosaur.com>

Based on code by:

Tom Molesworth <cpan@entitymodel.com>

=head1 LICENSE

Copyright 2014 Lucky Dinosaur, LLC. Licensed under the same terms as Perl itself.
Copyright Tom Molesworth 2013. Licensed under the same terms as Perl itself.


