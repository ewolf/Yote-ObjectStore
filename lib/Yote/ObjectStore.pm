package Yote::ObjectStore;

# TODO - perldoc explaining method signatures and methods
#      - Container --> Obj
#      - move lock out of RecordStore to its own thing

use strict;
use warnings;
no warnings 'uninitialized';

use Yote::ObjectStore::Array;
use Yote::ObjectStore::Container;
use Yote::ObjectStore::Hash;
use Yote::RecordStore::MySQL;

use Scalar::Util qw(weaken);
use Time::HiRes qw(time);

use constant {
    RECORD_STORE => 0,
    DIRTY => 1,
    WEAK  => 2,
    VOL   => 3,

    DATA  => 1,
};

=head1 NAME

Yote::ObjectStore - store and lazy load perl objects, hashes and arrays.

=head1 SYNOPSIS

 use Yote::ObjectStore;

 my $store = Yote::ObjectStore::open_store( '/path/to/data-directory' );

=head1 DESCRIPTION

Yote::ObjectStore

=head1 METHODS

=head2 open_store( $options )


=cut

sub open_object_store {
    my( $pkg, @args ) = @_;

    my %args = @args == 1 ? ( record_store => $args[0] ) : @args;
    my $record_store = $args{record_store};
    
    unless (ref $record_store) {
	return undef;
    }
    my $store = bless [
        $record_store,
        {},
        {},
        {},
        ], $pkg;
    return $store;
}

=head2 open_store( $options )

=cut

sub fetch_root {
    my $self = shift;
    my $record_store = $self->[RECORD_STORE];

    $record_store->lock;
    my $root_id = $self->[RECORD_STORE]->first_id();

    my $root = $self->fetch( $root_id );
    if ($root) {
        $record_store->unlock;
        return $root;
    }
    # if ($record_store->get_record_count > 0) {
    #     $record_store->unlock;
    #     $@ = __PACKAGE__."->fetch_root called on a record store that has entries but was unable to fetch the first record : $@ $!";
    #     return undef;
    # }

    $root = bless [ $root_id,
		    {},
		    $self,
		    {},
		    { created => time, updated => time } ], 'Yote::ObjectStore::Container';
    $record_store->stow( $root->__freezedry, $root_id );

    $self->weak( $root_id, $root );
    
    $record_store->unlock;
    return $root;
} #fetch_root


=head2 open_store( $options )


=cut

sub lock {
    my ($self, @keys) = @_;
# TODO : USE LOCK SERVER
    return $self->[RECORD_STORE]->lock(@keys);
}


=head2 open_store( $options )


=cut

sub unlock {
# TODO : USE LOCK SERVER
    return shift->[RECORD_STORE]->unlock;
}

=head2 empty_vol()


=cut
sub empty_vol {
    my $self = shift;
    my $vol = $self->[VOL];
    %$vol = ();
}


=head2 open_store( $options )


=cut

sub save {
    my ($self,$obj) = @_;
    my $record_store = $self->[RECORD_STORE];
#    $record_store->lock;
    $record_store->use_transaction;

    my $dirty = $self->[DIRTY];
    if ($obj) {
        # kind of dangerous to overall integrity
        # use with caution
        my $r = ref( $obj );
        if ($r eq 'ARRAY') {
            $obj = tied @$obj;
        }
        elsif ($r eq 'HASH') {
            $obj = tied %$obj;
        }
        my $id = $obj->id;
        
        $record_store->stow( $obj->__freezedry, $id );
        # unless( defined $record_store->stow( $self->_freezedry($obj,$id), $id ) ) {
        #     return undef;
        # }
        delete $dirty->{$id};
    } else {
        my @ids_to_save = keys %$dirty;

        for my $id (@ids_to_save) {
            my $obj = delete $dirty->{$id};
            if ($obj) {
                $obj = $obj->[0];
            }
            my $r = ref( $obj );
            if ($r eq 'ARRAY') {
                $obj = tied @$obj;
            }
            elsif ($r eq 'HASH') {
                $obj = tied %$obj;
            }
            my $froze = $obj->__freezedry;
            $record_store->stow( $froze, $id );
            #	    unless (defined $record_store->stow( $self->_freezedry($obj,$id), $id )) {
            #		return undef;
            #	    }
        }
        %$dirty = ();
    }
    $record_store->commit_transaction;
#    $record_store->unlock;
    return 1;
} #save


=head2 open_store( $options )


=cut

sub fetch {
    my ($self, $id) = @_;
    my $obj;
    if (exists $self->[DIRTY]{$id}) {
        $obj = $self->[DIRTY]{$id}[0];
    } else {
        $obj = $self->[VOL]{$id} || $self->[WEAK]{$id};
    }

    return $obj if $obj;
    
    my $record_store = $self->[RECORD_STORE];
    $record_store->lock;
    
    my ( $update_time, $creation_time, $record ) = $record_store->fetch( $id );
    unless (defined $record) {
        return undef;
    }
    $obj = $self->_reconstitute( $id, $record, $update_time, $creation_time );
    $self->weak( $id, $obj );
    $record_store->unlock;
    return $obj;
} #fetch


=head2 open_store( $options )


=cut

sub tied_obj {
    my ($self, $item) = @_;
    my $r = ref( $item );
    my $tied = $r eq 'ARRAY' ? tied @$item
	: $r eq 'HASH' ? tied %$item
	: $item;
    return $tied;
} #tied_obj


=head2 open_store( $options )


=cut

sub existing_id {
    my ($self, $item) = @_;
    my $r = ref( $item );
    if ($r eq 'ARRAY') {
        my $tied = tied @$item;
        if ($tied) {
            return $tied->id;
        }
	return undef;
    }
    elsif ($r eq 'HASH') {
        my $tied = tied %$item;
        if ($tied) {
            return $tied->id;
        }
	return undef;
    }
    elsif ($r && $item->isa( 'Yote::ObjectStore::Container' )) {
	return $item->id;
    }
    return undef;

} #existing_id


=head2 open_store( $options )


=cut

sub _id {
    my ($self, $item) = @_;
    my $r = ref( $item );
    if ($r eq 'ARRAY') {
        my $tied = tied @$item;
        if ($tied) {
            return $tied->id;
        }
        my $id = $self->_new_id;
	my @contents = @$item;
        @$item = ();  # this is where the leak was coming from
        tie @$item, 'Yote::ObjectStore::Array', $id, $self;
	push @$item, @contents;
        $self->weak( $id, $item );
        $self->dirty( $id );
        return $id;
    }
    elsif ($r eq 'HASH') {
        my $tied = tied %$item;
        if ($tied) {
            return $tied->id;
        }
        my $id = $self->_new_id;
	my %contents = %$item;
        %$item = ();  # this is where the leak was coming frmo
        tie %$item, 'Yote::ObjectStore::Hash', $id, $self;
        $self->weak( $id, $item );
	for my $key (keys %contents) {
	    $item->{$key} = $contents{$key};
	}
        $self->dirty( $id );
        return $id;
    }
    elsif ($r && $item->isa( 'Yote::ObjectStore::Container' )) {
	return $item->id;
    }
    return undef;

} #_id

sub weak {
    my ($self,$id,$ref) = @_;
    $self->[WEAK]{$id} = $ref;

    weaken( $self->[WEAK]{$id} );
}

sub is_dirty {
    my ($self,$obj) = @_;
    my $id = $self->_id( $obj );
    return defined( $self->[DIRTY]{$id} );
}

sub id_is_referenced {
    my ($self,$id) = @_;
    return defined( $self->[WEAK]{$id} );
}

sub register_vol {
    my ($self,$id,$obj) = @_;
    $self->[VOL]{$id} = $obj;
}

#
# Must think carefully about weak references.
# Will calling weak here cause trouble? Something
# with tied and weak?
#
sub dirty {
    my ($self,$id,$obj) = @_;
    unless ($self->[WEAK]{$id}) {
	$self->weak($id,$obj);
    }
    my $target = $self->[WEAK]{$id};

    my @dids = keys %{$self->[DIRTY]};

    my $tied = $self->tied_obj( $target );

    $self->[DIRTY]{$id} = [$target,$tied];
} #dirty

sub xform_in {
    my ($self,$item) = @_;
    my $r = ref( $item );
    if ($r) {
        return 'r' . $self->_id( $item );
    }
    elsif (defined $item) {
        return "v$item";
    }
    else {
        return 'u';
    }
} #xform_in

# used by Container,Array,Hash
sub xform_out {
    my ($self,$str) = @_;
    return undef unless $str;
    my $tag = substr( $str, 0, 1 );
    my $val = substr( $str, 1 );
    if ($tag eq 'r') {
        #reference
        my $item = $self->fetch( $val );
        return $item;
    }
    elsif ($tag eq 'v') {
        return $val;
    }
    return undef;
} #xform_out

sub _reconstitute {
    my ($self, $id, $data, $update_time, $creation_time ) = @_;

    my $class_length = unpack "I", $data;
    (undef, my $class) = unpack "I(a$class_length)", $data;

    my $clname = $class;
    $clname =~ s/::/\//g;

    require "$clname.pm";

    return $class->__reconstitute( $id, $data, $self, $update_time, $creation_time );

} #_reconstitute

sub _new_id {
    my $self = shift;
    return $self->[RECORD_STORE]->next_id;
} #_new_id

sub create_container {
    my ($self, $data, $class) = @_;
    unless (ref $data) {
	($class,$data) = ($data,$class);
    }
    $data  //= {};
    $class //= 'Yote::ObjectStore::Container';

    if( $class ne 'Yote::ObjectStore::Container' ) {
      my $clname = $class;
      $clname =~ s/::/\//g;

      require "$clname.pm";
    }
    
    my $id = $self->_new_id;

    my $obj = bless [
        $id,
        { map { $_ => $self->xform_in($data->{$_}) } keys %$data},
        $self,
	{},
        ], $class;
    $self->dirty( $id, $obj );
    $obj->_init;
    $self->weak( $id, $obj );

    return $obj;
} #create_container

sub vacuum {
    my ($self, $destination_store) = @_;

    unless ($destination_store) {
        $@ = "vacuum must be called with a destination store";
        return 0;
    }

    my $record_store = $self->[RECORD_STORE];
    $record_store->lock;

    my $silo = $record_store->open_silo( $record_store->directory . '/vacuum',
                                         "I" );
    $silo->empty_silo;
    $silo->ensure_entry_count( $record_store->entry_count );

    my $mark;
    $mark = sub {
        my $obj_id = shift;

        my ($id) = ( $obj_id =~ /^r(.*)/ );
        return unless defined $id;

        if ($silo->get_record( $id )->[0] == 1) {
            return;
        }

        $silo->put_record( $id, 1 );

        my $obj = $self->tied_obj( $self->fetch( $id ) );
        
        $destination_store->stow( $obj->__freezedry, $id );

        my $data = $obj->[DATA];
        if (ref($data) eq 'ARRAY') {
            map { $mark->($_) } @$data;
        } else {
            map { $mark->($_) } values %$data;
        }
    };

    # finds the data that should be copied over
    $mark->( "r" . $record_store->first_id );
    
    $record_store->unlock;

    return 1;

} #vacuum

"BUUG";

=head1 AUTHOR
       Eric Wolf        coyocanid@gmail.com

=head1 COPYRIGHT AND LICENSE

       Copyright (c) 2012 - 2020 Eric Wolf. All rights reserved.  This program is free software; you can redistribute it and/or modify it
       under the same terms as Perl itself.

=head1 VERSION
       Version 2.13  (Feb, 2020))

=cut

