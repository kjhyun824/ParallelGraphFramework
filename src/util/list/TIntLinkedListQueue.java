package util.list;

import gnu.trove.list.linked.TIntLinkedList;

public class TIntLinkedListQueue extends TIntLinkedList {

    public TIntLinkedListQueue() {
        super();
    }

    public int poll() {
        if (super.size() == 0) {
            return -1;
        } else {
            return super.removeAt(0);
        }
    }

    static class TIntLink {
        int value;
        TIntLink previous;
        TIntLink next;

        TIntLink(int value) {
            this.value = value;
        }

        public int getValue() {
            return this.value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public TIntLink getPrevious() {
            return this.previous;
        }

        public void setPrevious(TIntLink previous) {
            this.previous = previous;
        }

        public TIntLink getNext() {
            return this.next;
        }

        public void setNext(TIntLink next) {
            this.next = next;
        }
    }
}
