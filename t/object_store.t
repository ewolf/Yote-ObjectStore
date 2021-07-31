#!/usr/bin/perl
use strict;
use warnings;
no warnings 'uninitialized';

use lib './t/lib';
use lib './lib';

use Yote::ObjectStore;
use Yote::RecordStore::MySQL;

use Data::Dumper;
use File::Temp qw/ :mktemp tempdir /;
use Test::More;

use Tainer;


my %args = (
    user     => 'wolf',
    password => 'B00gerzaais',
    host     => 'localhost',
    port     => 3306,
    );
my $factory = Factory->new( %args );

$factory->setup;
# -------------------------------------------------------------

my $record_store = $factory->new_rs;
my $object_store = Yote::ObjectStore->open_object_store( $record_store );
is ($record_store->get_record_count, 0, 'no records in store');

my $r1 = $object_store->fetch_root;
ok( $r1, 'got a root' );
my $r2 = $object_store->fetch_root;
is ($r1, $r2, "single reference for fetch root" );
ok ($r1 == $r2, "same reference compared numerically" );
ok (!($r1 != $r2), "same reference compared as not equal numerically" );
ok (!($r1 == 1), "reference does not equal to a number" );
ok ($r1 != 1, "reference is not equal to a number" );
ok ($r1 eq $r2, "same reference compared as strings" );
ok (!($r1 ne $r2), "same reference compared as not equal strings" );
ok ($r1 != 1, "not equal to a number" );
ok (! ($r1 eq 1), "not equal to a string" );
ok ($r1 ne 1, "not the same as a number" );
$r1->get( 'burger', 'time' );
is ($r2->get( 'burger' ), 'time', 'get with default called' );
my $a1 = $r1->get_array( [ 1, 2, 3 ] );
is_deeply ($a1, [ 1, 2, 3 ], "get array with defaults" );
$object_store->save;
#$record_store->close_recordstore;

# -------------------------------------------------------------

$object_store = Yote::ObjectStore->open_object_store( $record_store );
$r1 = $object_store->fetch_root;
is ($r1->get( 'burger' ), 'time', 'get with default called after reload' );

is_deeply ($r1->get_array, [ 1, 2, 3 ], "get array with defaults" );

my $newo = $object_store->create_container;
$r1->set_someobj( $newo );
is_deeply ($r1->get_array, [ 1, 2, 3 ], "get array with defaults and a push (still)" );
is ($r1->add_to_array( $newo, undef, { foo => "Bar`Var"} ), 6, 'six items after add to array' );

is_deeply ($r1->get_array, [ 1, 2, 3, $newo, undef, { foo => "Bar`Var" } ], "get array with defaults and a push" );

$object_store->save;

# -------------------------------------------------------------

$object_store = Yote::ObjectStore->open_object_store( $record_store );
$r1 = $object_store->fetch_root;
my $firsto = $r1->get_someobj;
is_deeply ($r1->get_array->[5], { foo => "Bar`Var" }, "got hash from save" );
my $arry = $r1->get_array;
is_deeply( [splice @$arry, 1, 2, "BEEP", $object_store->create_container( { me => "first", and => [ qw( the gimmi gimmies ) ] } )], [ 2, 3 ], "splice results" );
my $obj = $arry->[2];
is (ref $obj, 'Yote::ObjectStore::Container', "obj is correct reference" );
is ( $obj->get( 'me' ), 'first', 'got obj me' );
is ( $obj->get_me, 'first', 'got obj me using get_me' );
is_deeply( $arry, [ 1, "BEEP", $obj, $firsto, undef, { foo => "Bar`Var" } ], "array now after splice" );

is ($arry->[100], undef, 'Nothing at array 100');
is (scalar(@$arry), 6, '6 items in the array' );
push @$arry, "DELME";
is_deeply( $arry, [ 1, "BEEP", $obj, $firsto, undef, { foo => "Bar`Var" }, 'DELME' ], "array now after push" );
my $hash = $arry->[5];
is (scalar(@$arry), 7, 'now 7 items in the array' );
$#$arry = 5;
is (scalar(@$arry), 6, 'back to 6 items in the array' );
is_deeply( $arry, [ 1, "BEEP", $obj, $firsto, undef, { foo => "Bar`Var" } ], "array after adjust size" );

is (shift (@$arry), 1, "shifted the first thing away" );
is (scalar(@$arry), 5, '5 items in the array after shift' );
is (unshift(@$arry), 5, "unshifted nothing does nothing" );
is (unshift(@$arry,undef), 6, "unshifted an undef onto the array" );
is (shift ( @$arry), undef, "shifted undef thing away" );

push @$arry, "DELMEAGAIN";
is (pop(@$arry), "DELMEAGAIN", "Popped thing off" );
my $newa = $r1->set_newarry( [] );
is (pop(@$newa),undef, "popped from empty array" );
is (push(@$newa),0, "pushed nothing is still nothing" );
is_deeply( $newa, [], "nothing in new array yet" );
ok (!exists $newa->[12], 'index 12 has nothing in new array' );
is (delete $newa->[12], undef, 'nothing to delete at index 12' );
is (scalar(@$newa), 0, 'still nothing in new array' );

is (push( @$newa, "A", "B", "C" ), 3, "push3d 3 things" );
is_deeply( $newa, [ "A", "B", "C" ], 'new array with 3' );
is (delete $newa->[1], "B", 'deleted something off' );
is_deeply( $newa, [ "A", undef, "C" ], 'two things and an undef now in array' );
ok (exists $newa->[2], 'third index has something' );
ok (!defined $newa->[1], 'second index is not filled' );
$newa->[1] = "BERROA";
is_deeply( $newa, [ "A", "BERROA", "C" ], 'array now with more stuff' );
$newa->[1] = "BERROA";
is_deeply( $newa, [ "A", "BERROA", "C" ], 'array now with same stuff' );

@$newa = ();
is_deeply( $newa, [], 'array now empty after clear' );
ok ($object_store->is_dirty($newa), 'newa is now dirty' );
$object_store->save;
ok (!$object_store->is_dirty($newa), 'newa not dirty after save' );

@$newa = ();
is_deeply( $newa, [], 'array now empty after clear' );
ok (!$object_store->is_dirty($newa), 'empty newa not dirty after clear' );

is_deeply( $hash, { foo => "Bar`Var" }, "simple hash again" );
ok( !$object_store->is_dirty($hash), 'hash is not dirty' );
$hash->{foo} = "Bar`Var";
ok( !$object_store->is_dirty($hash), 'hash is still not dirty' );
ok( exists $hash->{foo}, 'foo key exists' );
ok( ! exists $hash->{boo}, 'boo key does not exist' );
ok( !$object_store->is_dirty($hash), 'hash is still not dirty' );

$hash->{boo} = 123;
my @keys;
for my $key (keys %$hash) {
    push @keys, $key;
}
is_deeply( [sort @keys], [qw(boo foo)], "got keys" );

my @keyvals;
while ( my($key,$val) = (each %$hash)) {
    push @keyvals, "$key -> $val";
}
is_deeply( [sort @keyvals], ['boo -> 123','foo -> Bar`Var'], "got foreach stuff" );

%$hash = ();
is_deeply( $hash, {}, "hash after clear" );


# -------------------------------------------------------------

$object_store = Yote::ObjectStore->open_object_store( $record_store );
$r1 = $object_store->fetch_root;
$hash = $r1->get_array->[4];
is_deeply( $hash, { foo => "Bar`Var" }, "simple hash reloaded" );
delete $hash->{derp};
ok( !$object_store->is_dirty($hash), 'hash is not dirty after deleteing non-existant key' );
$hash->{derp} = "NOW";
ok( $object_store->is_dirty($hash), 'hash is dirty after adding key' );
$object_store->save;
delete $hash->{derp};
ok( $object_store->is_dirty($hash), 'hash is dirty after deleteing a key' );
$object_store->save;

is_deeply( $hash, { foo => "Bar`Var" }, "simple hash again again" );
%$hash = ();
ok( $object_store->is_dirty($hash), 'hash is dirty after clear' );
$object_store->save;
%$hash = ();
ok( !$object_store->is_dirty($hash), 'hash not dirty after clear but was empty' );

$hash->{foo} = "BAR`Bar`BZZ";
$object_store->save;

# -------------------------------------------------------------
$object_store = Yote::ObjectStore->open_object_store( $record_store );
$r1 = $object_store->fetch_root;
$hash = $r1->get_array->[4];

is_deeply( $hash, { foo => "BAR`Bar`BZZ" }, "simple hash again again" );
is ($r1->get( 'burger', 'not this time' ), 'time', "object got with default but had a value" );
is ($r1->get_nada, undef, 'No nada' );
ok( !$object_store->is_dirty($r1), 'root not dirty gets' );
is ($r1->set_burger('time'), 'time', 'set time again' );
ok( !$object_store->is_dirty($r1), 'root not dirty after setting field to same value' );

throws_ok ( sub { $r1->BLBLBLB },
	    qr/unknown function/,
	    'tried to call a method on root that does not exist' );

my $newoid;
{
    my $newo = $r1->get_someobj;
    $newoid = "$newo";
    ok ($object_store->id_is_referenced($newoid), "there is a weak reference to someobj" );
}
pass ('no error for $newo going out of scope');
ok (!$object_store->id_is_referenced($newoid), "there is no longer a weak reference to someobj" );

$r1->add_to_somearry( qw( ONE ) );

$arry = $r1->get_somearry;
is_deeply( $arry, [ qw( ONE ) ], "one on somearray" );
ok( $object_store->is_dirty($arry), 'somearray was just made and is dirty' );
$object_store->save;
$r1->add_once_to_somearry( qw( FOO ONE ) );
is_deeply( $arry, [ qw( ONE FOO ) ], "still one on somearray" );
ok( $object_store->is_dirty($arry), 'somearray changed and dirty' );
$object_store->save;
$r1->add_once_to_somearry( qw( FOO ONE ) );
is_deeply( $arry, [ qw( ONE FOO ) ], "still one foo on somearray" );
ok( !$object_store->is_dirty($arry), 'somearray not changed nor dirty' );

$r1->add_to_somearry( qw( FOO FOO BOO FOO ) );
is_deeply( $arry, [ qw( ONE FOO FOO FOO BOO FOO ) ], "still one on somearray" );
$r1->remove_from_somearry( qw( FOO ) );
is_deeply( $arry, [ qw( ONE FOO FOO BOO FOO ) ], "removed one foo" );

$r1->remove_all_from_somearry( qw( FOO ) );
is_deeply( $arry, [qw( ONE BOO )], "removed all foo" );

$r1->set_tainer( $object_store->create_container( {}, 'Tainer' ) );
is ($r1->get_tainer->nice, "YES", "a nice tainer" );

$object_store->save;

# -------------------------------------------------------------
$object_store = Yote::ObjectStore->open_object_store( $record_store );
is ( $object_store->fetch_root->get_tainer->nice, "YES", "a nice tainer" );

is ( $record_store->get_record_count, 9, "9 records" );
is ( $object_store->_new_id, 10, '10th id' );
is ( $object_store->fetch( 10 ), undef, 'no object yet at 10' );
#$object_store->close_objectstore;

# -------------------------------------------------------------

$record_store = $factory->new_rs;
$object_store = Yote::ObjectStore->open_object_store( $record_store );
my $root = $object_store->fetch_root;
my $arr = $root->get_arry([]);
$arr->[5] = "HITHE";
push @$arr, {}, {A => 1}, undef, 'C', [];
is_deeply( $arr, [ undef, undef, undef, undef, undef, 'HITHE', {}, {A => 1}, undef, 'C', [] ], 'arry got filled with stuff' );
$object_store->save;
#$object_store->close_objectstore;

my $os2 = Yote::ObjectStore->open_object_store( $record_store );
my $arry2 = $os2->fetch_root->get_arry;
is_deeply( $arry2, [ undef, undef, undef, undef, undef, 'HITHE',  {}, {A => 1}, undef, 'C', [] ], 'arry repopened filled with stuff' );

#$os2->close_objectstore;

# -------------------------------------------------------------

$record_store = $factory->new_rs;
$object_store = Yote::ObjectStore->open_object_store( $record_store );
$root = $object_store->fetch_root;
$root->set_fredholder( $object_store->create_container( 'GetInLoad' ));
$object_store->save;
$os2 = Yote::ObjectStore->open_object_store( $record_store );
$arry2 = $os2->fetch_root->get_fredholder->get_fred;

# this will fail explosivly if dirty is not handled properly
$os2->save;

is_deeply( $arry2, [], 'fred did get saved' );
#$os2->close_objectstore;

# -------------------------------------------------------------

$record_store = $factory->reopen( $record_store );
$os2 = Yote::ObjectStore->open_object_store( $record_store );
my $recur = $os2->fetch_root->get_recur({});
my $o = $os2->create_container;
$arry = [];
$hash = { obj => $o, arry => $arry };
$hash->{hash} = $hash;
push @$arry, $arry, $hash, $o;
$o->set_hash( $hash );
$o->set_arry( $arry );
$o->set_obj( $o );
$recur->{recur} = $arry;
$os2->save;
#$os2->close_objectstore;

$os2 = Yote::ObjectStore->open_object_store( $record_store );
$arry = $os2->fetch_root->get_recur->{recur};
my( $acopy, $hashcopy, $ocopy ) = @$arry;
is( $arry, $acopy, "array copy copy" );
is( $hashcopy, $hashcopy->{hash}, "hash copy copy" );
is( $hashcopy->{obj}, $hashcopy->{obj}->get_obj, "obj copy copy" );

#$os2->close_objectstore;

done_testing();

exit;



sub throws_ok {
    my ( $subr, $erex, $msg ) = @_;
    eval {
	$subr->();
	fail( $msg );
    };
    like( $@, $erex, $msg );
} #throws_ok

package Factory;

sub new_db_name {
    my ( $self ) = @_;

    my $args = $self->{args};
    my $dsn = "DBI:mysql:host=$args->{host}:port=$args->{port}";
    my $dbh = DBI->connect( $dsn, $args->{user}, $args->{password} );
    $self->{dbh} = $dbh;
    die "No db : $@, $!" unless $dbh;
    my $tries = 0;
    while ($tries < 10) {
	my $dbn = "test_$tries";

        my $rv = $dbh->do( "SHOW DATABASES LIKE '$dbn'" );
        if ($rv == 0) {
            $dbh->do( "CREATE DATABASE $dbn" );
	     $self->{dbnames}{$dbn} = 1;
	     return $dbn;
        }
	$tries++;
    }
    $@ = "gave up after $tries tries";
    print STDERR "NOPE : $@\n";
    return undef;
} #new_db_name

sub new {
    my ($pkg, %args) = @_;
    return bless { args => {%args}, dbnames => {} }, $pkg;
}

sub new_rs {
    my ($self) = @_;
    
    # make a test db
    $self->{args}{database} = $self->new_db_name;
    my $store = Yote::RecordStore::MySQL->open_store( %{$self->{args}} );
    return $store;
}
sub reopen {
    my( $cls, $oldstore ) = @_;
    my $options = $oldstore->{options};
    return Yote::RecordStore::MySQL->open_store( %$options );
}
sub teardown {
    my $self = shift;
    for my $dbn (keys %{$self->{dbnames}}) {
	$self->{dbh}->do( "DROP DATABASE IF EXISTS $dbn" );
    }
}
sub setup {
    my $self = shift;
    my $args = $self->{args};
    my $dsn = "DBI:mysql:host=$args->{host}:port=$args->{port}";
    my $dbh = DBI->connect( $dsn, $args->{user}, $args->{password} );
    for (0..10) {
        $dbh->do( "DROP DATABASE IF EXISTS test_$_" );
    }
    warn "get rid of this for producgtion";
}
