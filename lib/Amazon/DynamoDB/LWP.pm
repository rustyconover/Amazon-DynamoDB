package Amazon::DynamoDB::LWP;

use strict;
use warnings;

=head1 NAME

Amazon::DynamoDB::LWP - make requests using L<LWP::UserAgent>

=head1 DESCRIPTION

Provides a L</request> method which will use L<LWP::UserAgent> to make
requests and return a L<Future> containing the result. Used internally by
L<Amazon::DynamoDB>.

=cut

use Future;
use LWP::UserAgent;

=head2 new

Instantiate.

=cut

sub new { my $class = shift; bless {@_}, $class }

=head2 request

Issues the request. Expects a single L<HTTP::Request> object,
and returns a L<Future> which will resolve to the decoded
response content on success, or the failure reason on failure.

=cut

sub request {
	my $self = shift;
	my $req = shift;
	my $resp = $self->ua->request($req);
	return Future->new->done($resp->decoded_content) if $resp->is_success;

	my $status = join ' ', $resp->code, $resp->message;
	return Future->new->fail($status, $resp, $req)
}

=head2 ua

Returns the L<LWP::UserAgent> instance.

=cut

sub ua { shift->{ua} ||= LWP::UserAgent->new(keep_alive => 10,
                                             agent => 'Amazon::DynamoDB/1.0',
                                             timeout => 90,
                                         ); }


=head2 delay

Waits for a given interval of seconds.

Take the number of seconds to wait as a parameter.  Used for retrying requests.

=cut

sub delay {
    my $self = shift;
    my $amount = shift;

    Future->call(sub {
                     # Sleep could be less than one second, so use select.
                     if ($amount > 0) {
                         select(undef, undef, undef, $amount);
                     }
                     Future->new->done();
                 });
}



1;

